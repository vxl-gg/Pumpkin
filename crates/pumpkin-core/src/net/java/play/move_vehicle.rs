#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_move_vehicle(&self, player: &Arc<Player>, packet: &SMoveVehicle) {
        // A movement packet was received this tick — tracked for SClientTickEnd zeroing.
        self.received_movement_this_tick
            .store(true, Ordering::Relaxed);
        let entity = player.get_entity();
        let last_pos = entity.pos.load();
        let pos = Vector3::new(packet.x, packet.y, packet.z);
        let mut final_pos = pos;
        let mut other_player_passengers = Vec::new();
        let vehicle = entity
            .vehicle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(vehicle) = vehicle {
            let vehicle_entity = vehicle.get_entity();
            let from = vehicle_entity.pos.load();
            vehicle_entity.set_pos(pos);
            vehicle_entity.set_rotation(packet.yaw, packet.pitch);

            if let Some(vehicle) = vehicle.get_vehicle_entity() {
                let move_result = vehicle.move_vehicle(from, pos);
                final_pos = move_result.position;
                vehicle_entity.set_pos(final_pos);

                let passengers = vehicle_entity
                    .passengers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let world = vehicle_entity.world.load();
                for passenger in passengers {
                    let passenger_entity = passenger.get_entity();
                    passenger_entity.set_pos(final_pos);
                    if passenger_entity.entity_id != entity.entity_id
                        && let Some(player) = world.get_player_by_id(passenger_entity.entity_id)
                    {
                        other_player_passengers.push(player);
                    }
                }

                if move_result.cancelled {
                    vehicle_entity.velocity.store(Vector3::new(0.0, 0.0, 0.0));
                    vehicle_entity.send_velocity();
                }

                if final_pos != pos {
                    self.try_send_packet(&CMoveVehicle::new(
                        final_pos.x,
                        final_pos.y,
                        final_pos.z,
                        vehicle_entity.yaw.load(),
                        vehicle_entity.pitch.load(),
                    ));
                }
            }

            let world = player.world();
            let vehicle_pos = final_pos.to_block_pos();
            let check_below = BlockPos::new(vehicle_pos.0.x, vehicle_pos.0.y - 1, vehicle_pos.0.z);
            let in_lava = world.get_block(&vehicle_pos) == &pumpkin_data::Block::LAVA
                || world.get_block(&check_below) == &pumpkin_data::Block::LAVA;

            if in_lava {
                let start_pos = player.entered_lava_pos.load().unwrap_or(final_pos);
                if player.entered_lava_pos.load().is_none() {
                    player.entered_lava_pos.store(Some(final_pos));
                }
                let dx = final_pos.x - start_pos.x;
                let dz = final_pos.z - start_pos.z;
                let distance_horizontal = dx.hypot(dz);
                player.trigger_advancement(
                    crate::entity::player::advancement::trigger::AdvancementTrigger::RideEntityInLava {
                        distance_horizontal,
                        dimension: world.dimension.minecraft_name.to_string(),
                        vehicle_type: Some(format!(
                            "minecraft:{}",
                            vehicle_entity.entity_type.resource_name
                        )),
                    },
                );
            } else {
                player.entered_lava_pos.store(None);
            }
        } else {
            player.entered_lava_pos.store(None);
        }
        entity.set_pos(final_pos);
        for passenger in other_player_passengers {
            chunker::update_position(&passenger);
        }
        let distance = last_pos.squared_distance_to_vec(&final_pos).sqrt();
        let cm = (distance * 100.0).round() as i32;
        if cm > 0 {
            let stat = player.get_movement_statistic();
            player.increment_stat(
                pumpkin_data::statistic::StatisticCategory::Custom,
                stat as i32,
                cm,
            );
        }
        chunker::update_position(player);
    }
}
