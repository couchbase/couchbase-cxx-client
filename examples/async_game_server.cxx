/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/logger.hxx>

#include <spdlog/fmt/bundled/core.h>

#include <couchbase/fmt/error.hxx>

#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <thread>

using namespace couchbase::transactions;

std::string
make_uuid()
{
  static std::random_device dev;
  static std::mt19937 rng(dev());

  std::uniform_int_distribution<int> dist(0, 15);

  const char* v = "0123456789abcdef";
  const std::array dash_pattern{
    false, false, false, false, true,  false, true,  false,
    true,  false, true,  false, false, false, false, false,
  };

  std::string res;
  for (bool dash : dash_pattern) {
    if (dash) {
      res += "-";
    }
    res += v[dist(rng)];
    res += v[dist(rng)];
  }
  return res;
}

struct Player {
  int experience;
  int hitpoints;
  std::string json_type;
  int level;
  bool logged_in;
  std::string name;
  std::string uuid;
};

template<>
struct tao::json::traits<Player> {
  template<template<typename...> class Traits>
  static void assign(tao::json::basic_value<Traits>& v, const Player& p)
  {
    v = { { "experience", p.experience },
          { "hitpoints", p.hitpoints },
          { "jsonType", p.json_type },
          { "level", p.level },
          { "loggedIn", p.logged_in },
          { "name", p.name },
          { "uuid", p.uuid } };
  }

  template<template<typename...> class Traits>
  static void to(const tao::json::basic_value<Traits>& v, Player& p)
  {
    v.at("experience").to(p.experience);
    v.at("hitpoints").to(p.hitpoints);
    v.at("jsonType").to(p.json_type);
    v.at("level").to(p.level);
    v.at("loggedIn").to(p.logged_in);
    v.at("name").to(p.name);
    v.at("uuid").to(p.uuid);
  }
};

struct Monster {
  int experience_when_killed;
  int hitpoints;
  double item_probability;
  std::string json_type;
  std::string name;
  std::string uuid;
};

template<>
struct tao::json::traits<Monster> {
  template<template<typename...> class Traits>
  static void assign(tao::json::basic_value<Traits>& v, const Monster& m)
  {
    v = { { "experienceWhenKilled", m.experience_when_killed },
          { "hitpoints", m.hitpoints },
          { "itemProbability", m.item_probability },
          { "jsonType", m.json_type },
          { "name", m.name },
          { "uuid", m.uuid } };
  }

  template<template<typename...> class Traits>
  static void to(const tao::json::basic_value<Traits>& v, Monster& m)
  {
    v.at("experienceWhenKilled").to(m.experience_when_killed);
    v.at("hitpoints").to(m.hitpoints);
    v.at("itemProbability").to(m.item_probability);
    v.at("jsonType").to(m.json_type);
    v.at("name").to(m.name);
    v.at("uuid").to(m.uuid);
  }
};

class GameServer
{
private:
  std::shared_ptr<transactions> transactions_;

public:
  explicit GameServer(couchbase::cluster& cluster)
    : transactions_(cluster.transactions())
  {
  }

  [[nodiscard]] int calculate_level_for_experience(int experience) const
  {
    return experience / 100;
  }

  std::future<std::pair<couchbase::error, transaction_result>> player_hits_monster(
    int damage,
    const couchbase::collection& collection,
    const std::string& player_id,
    const std::string& monster_id,
    std::atomic<bool>& exists)
  {
    auto barrier =
      std::make_shared<std::promise<std::pair<couchbase::error, transaction_result>>>();
    auto f = barrier->get_future();
    transactions_->run(
      [this, damage, collection, player_id, monster_id, &exists](
        std::shared_ptr<async_attempt_context> ctx) -> couchbase::error {
        ctx->get(
          collection,
          monster_id,
          [ctx, this, collection, monster_id, player_id, &exists, damage = std::move(damage)](
            auto e, auto monster) {
            if (e.ec() == couchbase::errc::transaction_op::document_not_found) {
              std::cout << "monster no longer exists" << std::endl;
              exists = false;
              return;
            }
            auto monster_body = monster.template content_as<Monster>();
            auto monster_hitpoints = monster_body.hitpoints;
            auto monster_new_hitpoints = monster_hitpoints - damage;

            std::cout << "Monster " << monster_id << " had " << monster_hitpoints
                      << " hitpoints, took " << damage << " damage, now has "
                      << monster_new_hitpoints << " hitpoints" << std::endl;
            if (monster_new_hitpoints <= 0) {
              // Monster is killed. The remove is just for demoing, and a more realistic examples
              // would set a "dead" flag or similar.
              ctx->remove(monster, [](auto e) {
                if (e.ec()) {
                  std::cout << "error removing monster: " << e.ec().message() << std::endl;
                }
              });
              // also, in parallel, get/update player
              ctx->get(
                collection,
                player_id,
                [ctx, player_id, monster_id, monster_body, this](auto e, auto player) {
                  if (e.ec()) {
                    std::cout << "error getting player: " << e.ec().message() << std::endl;
                    return;
                  }
                  const Player& player_body = player.template content_as<Player>();

                  // the player earns experience for killing the monster
                  int experience_for_killing_monster = monster_body.experience_when_killed;
                  int player_experience = player_body.experience;
                  int player_new_experience = player_experience + experience_for_killing_monster;
                  int player_new_level = calculate_level_for_experience(player_new_experience);

                  std::cout << "Monster " << monster_id << " was killed. Player " << player_id
                            << " gains " << experience_for_killing_monster
                            << " experience, now has level " << player_new_level << std::endl;

                  Player player_new_body = player_body;
                  player_new_body.experience = player_new_experience;
                  player_new_body.level = player_new_level;
                  ctx->replace(player, player_new_body, [](auto e, auto) {
                    if (e.ec()) {
                      std::cout << "Error updating player :" << e.ec().message() << std::endl;
                    }
                  });
                });
            } else {
              std::cout << "Monster " << monster_id << " is damaged but alive" << std::endl;

              Monster monster_new_body = monster_body;
              monster_new_body.hitpoints = monster_new_hitpoints;
              ctx->replace(monster, monster_new_body, [monster_new_body](auto e, auto res) {
                if (e.ec()) {
                  std::cout << "Error updating monster :" << e.ec().message() << std::endl;
                } else {
                  auto body = couchbase::codec::tao_json_serializer::serialize(monster_new_body);
                  std::cout << "Monster body updated to :"
                            << std::string(reinterpret_cast<char*>(&body.front()), body.size())
                            << std::endl;
                }
              });
            }
          });
        return {};
      },
      [barrier](auto err, auto res) {
        barrier->set_value({ err, res });
      });
    return f;
  }
};

int
main()
{
  couchbase::logger::initialize_console_logger();
  couchbase::logger::set_level(couchbase::logger::log_level::trace);

  const int NUM_THREADS = 4;
  std::atomic<bool> monster_exists = true;
  std::string bucket_name = "default";

  std::uniform_int_distribution<int> hit_distribution(1, 6);
  std::mt19937 random_number_engine; // pseudorandom number generator
  auto rand = std::bind(hit_distribution, random_number_engine);

  auto options = couchbase::cluster_options("Administrator", "password");
  options.transactions().cleanup_config().cleanup_window(std::chrono::seconds(60));
  options.transactions().durability_level(couchbase::durability_level::majority);
  options.transactions().cleanup_config().cleanup_lost_attempts(true);
  options.transactions().cleanup_config().cleanup_client_attempts(true);

  auto [connect_err, cluster] = couchbase::cluster::connect("couchbase://localhost", options).get();
  if (connect_err) {
    std::cout << "Error opening cluster: " << fmt::format("{}", connect_err) << std::endl;
    return -1;
  }

  auto collection = cluster.bucket("default").default_collection();

  std::string player_id{ "player_data" };
  Player player_data{ 14248, 23832, "player", 141, true, "Jane", make_uuid() };

  std::string monster_id{ "a_grue" };
  Monster monster_data{ 91, 4000, 0.19239324085462631, "monster", "Grue", make_uuid() };

  // upsert a player document
  {
    auto [err, resp] = collection.upsert(player_id, player_data, {}).get();
    if (!err) {
      std::cout << "Upserted sample player document: " << player_id
                << "with cas:" << resp.cas().value() << std::endl;
    }
  }
  // upsert a monster document
  {
    auto [err, resp] = collection.upsert(monster_id, monster_data, {}).get();
    if (!err) {
      std::cout << "Upserted sample monster document: " << monster_id
                << "with cas:" << resp.cas().value() << std::endl;
    }
  }

  GameServer game_server(cluster);
  std::vector<std::thread> threads;
  threads.reserve(NUM_THREADS);
  for (int i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back(
      [&rand, player_id, collection, monster_id, &monster_exists, &game_server]() {
        while (monster_exists.load()) {
          try {
            std::cout << "[thread " << std::this_thread::get_id()
                      << "]Monster exists -- lets hit it!" << std::endl;

            auto [err, res] =
              game_server
                .player_hits_monster(rand() % 80, collection, player_id, monster_id, monster_exists)
                .get();
            if (!err.ec()) {
              std::cout << "[thread " << std::this_thread::get_id() << "] success" << std::endl;
            } else {
              std::cout << "[thread " << std::this_thread::get_id() << "] " << err.ec().message()
                        << std::endl;
            }
          } catch (const std::exception& e) {
            std::cout << "[thread " << std::this_thread::get_id() << "] got exception " << e.what()
                      << std::endl;
          }
        }
      });
  }

  // wait for all threads to finish...
  for (auto& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }
  // close the cluster...
  cluster.close().get();
}
