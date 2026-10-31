use std::{
    error::Error,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use arc_swap::ArcSwap;
use pumpkin_config::{AdvancedConfiguration, BasicConfiguration, TelemetryConfig};
use pumpkin_data::packet::CURRENT_MC_VERSION;
use pumpkin_protocol::{
    ConnectionState,
    java::{client::play::CKeepAlive, packet_decoder::TCPNetworkDecoder},
    packet::MultiVersionJavaPacket,
    ser::NetworkReadExt,
};
use pumpkin_util::GameMode;
use tokio::io::BufReader;
use uuid::Uuid;

use super::*;
use crate::{
    data::VanillaData,
    entity::player::CustomScoreboard,
    net::{
        ClientPlatform, GameProfile, PacketRateLimiter, PlayerConfig,
        java::{JavaClient, pending::PendingConnection},
    },
    server::Server,
};

fn populate(board: &mut Scoreboard, target: &impl ScoreboardTarget) {
    board.add_objective(
        target,
        ScoreboardObjective::new(
            "voxel_board",
            TextComponent::text("Voxel Network"),
            RenderType::Integer,
            Some(NumberFormat::Blank),
            "dummy",
        ),
    );
    board.update_score(
        target,
        ScoreboardScore::new("entry", "voxel_board", VarInt(7), None, None),
    );
    board.add_team(
        target,
        Team {
            name: "voxel_line".into(),
            display_name: TextComponent::text(""),
            options: 0,
            nametag_visibility: NameTagVisibility::Always,
            collision_rule: CollisionRule::Always,
            color: NamedColor::White,
            player_prefix: TextComponent::text("Online players: "),
            player_suffix: TextComponent::text(""),
            players: vec!["entry".into()],
        },
    );
    board.set_display_objective(target, ScoreboardDisplaySlot::Sidebar, Some("voxel_board"));
}

fn player_with_transport(
    server: &Arc<Server>,
    world: &Arc<World>,
) -> Result<(Arc<Player>, tokio::io::DuplexStream), Box<dyn Error>> {
    let (transport, peer) = tokio::io::duplex(8192);
    let (read, write) = tokio::io::split(transport);
    let pending = PendingConnection::from_transport(
        Box::new(read),
        Box::new(write),
        "127.0.0.1:25565".parse()?,
        0,
        PacketRateLimiter::new(false, 0.0, 0.0),
        Arc::downgrade(server),
    );
    let profile = GameProfile {
        id: Uuid::new_v4(),
        name: "ScoreboardTest".into(),
        properties: ArcSwap::from_pointee(vec![]),
        profile_actions: None,
    };
    let mut client = JavaClient::from_pending(pending, profile.clone(), PlayerConfig::default());
    client.connection_state.store(ConnectionState::Play);
    client.start_outgoing_packet_task();
    let player = Arc::new(Player::new(
        Arc::new(ClientPlatform::Java(client)),
        profile,
        PlayerConfig::default(),
        world,
        GameMode::Survival,
    ));
    Ok((player, peer))
}

async fn read_scoreboard_packets(
    peer: tokio::io::DuplexStream,
) -> Result<Vec<pumpkin_protocol::RawPacket>, Box<dyn Error>> {
    let mut decoder = TCPNetworkDecoder::new(BufReader::new(peer));
    let packets = tokio::time::timeout(Duration::from_secs(30), async {
        let mut packets = vec![];
        loop {
            let packet = decoder.get_raw_packet().await?;
            if packet.id == CKeepAlive::to_id(CURRENT_MC_VERSION) {
                break;
            }
            packets.push(packet);
        }
        Ok::<_, pumpkin_protocol::PacketDecodeError>(packets)
    })
    .await??;
    Ok(packets)
}

#[tokio::test]
async fn login_sends_preexisting_scoreboard_once() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let basic = BasicConfiguration {
        default_level_name: directory
            .path()
            .join("world")
            .to_string_lossy()
            .into_owned(),
        allow_nether: false,
        allow_end: false,
        ..Default::default()
    };
    let mut advanced = AdvancedConfiguration::default();
    advanced.networking.bedrock.online_mode = false;
    let server = Server::new(
        basic,
        advanced,
        TelemetryConfig::default(),
        VanillaData::default(),
        vec![],
    )
    .await?;
    let world = server.worlds.load().first().cloned().ok_or("No world")?;

    for custom in [false, true] {
        let (player, peer) = player_with_transport(&server, &world)?;
        world.add_player(&player)?;
        let client = player.client.java().ok_or("No Java client")?;

        if custom {
            let mut board = Scoreboard::default();
            populate(&mut board, &player);
            player
                .as_java()
                .ok_or("No Java player")?
                .set_scoreboard(Some(board));
        } else {
            let mut board = world
                .scoreboard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            populate(&mut board, &world);
        }
        // No writer task can run on this current-thread runtime until the next await.
        assert_eq!(client.pending_bytes.load(Ordering::Acquire), 0);

        player.initialize_scoreboard();
        let snapshot_bytes = client.pending_bytes.load(Ordering::Acquire);
        assert!(snapshot_bytes > 0);
        player.initialize_scoreboard();
        assert_eq!(client.pending_bytes.load(Ordering::Acquire), snapshot_bytes);

        if custom {
            let mut guard = player
                .custom_scoreboard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(CustomScoreboard::Java(board)) = guard.as_mut() {
                board.update_score(
                    &player,
                    ScoreboardScore::new("entry", "voxel_board", VarInt(9), None, None),
                );
            }
        } else {
            world
                .scoreboard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .update_score(
                    &world,
                    ScoreboardScore::new("entry", "voxel_board", VarInt(9), None, None),
                );
        }
        client.try_send_packet(&CKeepAlive::new(1));

        let packets = read_scoreboard_packets(peer).await?;
        assert_eq!(
            packets.iter().map(|packet| packet.id).collect::<Vec<_>>(),
            [
                CUpdateObjectives::to_id(CURRENT_MC_VERSION),
                CDisplayObjective::to_id(CURRENT_MC_VERSION),
                CUpdateScore::to_id(CURRENT_MC_VERSION),
                CSetPlayerTeam::to_id(CURRENT_MC_VERSION),
                CUpdateScore::to_id(CURRENT_MC_VERSION),
            ]
        );
        let scores: Vec<_> = packets
            .iter()
            .filter(|p| p.id == CUpdateScore::to_id(CURRENT_MC_VERSION))
            .map(|p| {
                let mut payload = &p.payload[..];
                let _ = payload.get_str()?;
                let _ = payload.get_str()?;
                Ok::<_, pumpkin_protocol::ser::ReadingError>(payload.get_var_int()?.0)
            })
            .collect::<Result<_, _>>()?;
        assert_eq!(scores, [7, 9]);

        client.close();
        client.await_tasks().await;
        let _ = world.remove_player(&player, false).await;
    }
    server.stop();
    server.shutdown().await;
    Ok(())
}

#[test]
fn bedrock_row_ids_survive_updates_and_distinguish_objectives() {
    let original = bedrock_score_entry("§fOnline players: 12", "voxel_board", 7);
    let name = String::from("§fOnline players: 12");
    let updated = bedrock_score_entry(&name, "voxel_board", 6);
    let removed = bedrock_score_entry(&name, "voxel_board", 0);
    assert_eq!(original.scoreboard_id, updated.scoreboard_id);
    assert_eq!(original.scoreboard_id, removed.scoreboard_id);
    assert_ne!(
        original.scoreboard_id,
        bedrock_score_entry(&name, "other_board", 7).scoreboard_id
    );
    assert_ne!(
        original.scoreboard_id,
        bedrock_score_entry("§fRank: None", "voxel_board", 7).scoreboard_id
    );
    assert_eq!(original.objective_name, "voxel_board");
    assert_eq!(original.custom_name, name);
    assert_eq!(original.score.0, 7);
    assert_eq!(original.entry_type.0, 3);
}

#[test]
fn bedrock_objective_refresh_restores_rows_after_all_displays() -> Result<(), Box<dyn Error>> {
    let rows = [
        ("§0", 8),
        ("§fOnline players: 12", 7),
        ("§2", 6),
        ("§fRank: §4Owner", 5),
        ("§fPing: 0 ms", 4),
        ("§5", 3),
        ("§fWebsite and IP:", 2),
        ("§evxl.gg", 1),
    ];
    let mut board = BedrockScoreboardBuilder::new()
        .objective("voxel_board", "Voxel Network", BedrockSortOrder::Descending)
        .objective("other", "Other board", BedrockSortOrder::Ascending)
        .display_slot(BedrockDisplaySlot::Sidebar, "voxel_board")
        .display_slot(BedrockDisplaySlot::PlayerList, "voxel_board")
        .score("unrelated", "other", 99)
        .build();
    for (name, value) in rows {
        board
            .scores
            .insert((name.into(), "voxel_board".into()), value);
    }
    let scores = board.scores.clone();
    let mut previous_ids = None;
    for title in [
        "§b§lVoxel Network",
        "§f§lVoxel Network",
        "§b§lVoxel Network",
    ] {
        board
            .objectives
            .get_mut("voxel_board")
            .ok_or("Missing objective")?
            .display_name = title.into();
        let packets = board.objective_packets("voxel_board");
        let [
            BedrockObjectivePacket::Remove(remove),
            BedrockObjectivePacket::Display(first),
            BedrockObjectivePacket::Display(second),
            BedrockObjectivePacket::Scores(scores),
        ] = packets.as_slice()
        else {
            return Err("Expected remove, both displays, then restored scores".into());
        };
        assert_eq!(remove.objective_name, "voxel_board");
        let mut slots = [
            first.display_slot_name.as_str(),
            second.display_slot_name.as_str(),
        ];
        slots.sort_unstable();
        assert_eq!(slots, ["list", "sidebar"]);
        for display in [first, second] {
            assert_eq!(display.objective_name, "voxel_board");
            assert_eq!(display.objective_display_name, title);
            assert_eq!(display.sort_order.0, 1);
        }
        assert_eq!(scores.action.0, 0);
        assert_eq!(scores.entries.len(), rows.len());
        let mut ids = FxHashMap::default();
        for entry in &scores.entries {
            assert_eq!(entry.objective_name, "voxel_board");
            let expected = rows
                .iter()
                .find(|(name, _)| *name == entry.custom_name)
                .ok_or("Unexpected score row")?;
            assert_eq!(entry.score.0, expected.1);
            assert_eq!(entry.entry_type.0, 3);
            ids.insert(entry.custom_name.clone(), entry.scoreboard_id);
        }
        if let Some(previous) = previous_ids {
            assert_eq!(ids, previous);
        }
        previous_ids = Some(ids);
    }
    assert_eq!(board.scores, scores);
    Ok(())
}
