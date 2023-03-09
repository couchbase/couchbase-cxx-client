#!/usr/bin/env ruby

#  Copyright 2020-2021 Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

require "fileutils"
require "rbconfig"

def which(name)
  ENV.fetch("PATH", "")
     .split(File::PATH_SEPARATOR)
     .map { |path| [path, name].join(File::SEPARATOR) + RbConfig::CONFIG["EXEEXT"] }
     .find { |file| File.executable?(file) }
end

def run(*args)
  args = args.compact.map(&:to_s)
  puts args.join(" ")
  system(*args) || abort("command returned non-zero status: #{args.join(" ")}")
end

PROJECT_ROOT = Dir.pwd

CB_DEFAULT_CC = "cc"
CB_DEFAULT_CXX = "c++"
CB_CLANG = ENV.fetch("CB_CLANG", "clang")
CB_CLANGXX = ENV.fetch("CB_CLANGXX", "clang++")

CB_SANITIZER = ENV.fetch("CB_SANITIZER", "")
unless CB_SANITIZER.empty?
  CB_DEFAULT_CC = CB_CLANG
  CB_DEFAULT_CXX = CB_CLANGXX
end

CB_CMAKE = ENV.fetch("CB_CMAKE", which("cmake"))
CB_CC = ENV.fetch("CB_CC", which(CB_DEFAULT_CC))
CB_CXX = ENV.fetch("CB_CXX", which(CB_DEFAULT_CXX))
CB_NUMBER_OF_JOBS = ENV.fetch("CB_NUMBER_OF_JOBS", "1").to_i
CB_CMAKE_BUILD_TYPE = ENV.fetch("CB_CMAKE_BUILD_TYPE", "Debug")
CB_CACHE_OPTION = ENV.fetch("CB_CACHE_OPTION", "ccache")

puts "RUBY_PLATFORM=#{RUBY_PLATFORM}"
puts "CB_CMAKE=#{CB_CMAKE}"
puts "CB_CMAKE_BUILD_TYPE=#{CB_CMAKE_BUILD_TYPE}"
puts "CB_NUMBER_OF_JOBS=#{CB_NUMBER_OF_JOBS}"
puts "CB_SANITIZER=#{CB_SANITIZER}"
if RUBY_PLATFORM !~ /mswin|mingw/
  puts "CB_CC=#{CB_CC}"
  puts "CB_CXX=#{CB_CXX}"
end
puts "CB_CACHE_OPTION=#{CB_CACHE_OPTION}"

CB_CMAKE_EXTRAS = []
case CB_SANITIZER
when /asan|address/
  CB_CMAKE_EXTRAS << "-DENABLE_SANITIZER_ADDRESS=ON"
when /lsan|leak/
  CB_CMAKE_EXTRAS << "-DENABLE_SANITIZER_LEAK=ON"
when /ubsan|undefined_behaviour/
  CB_CMAKE_EXTRAS << "-DENABLE_SANITIZER_UNDEFINED_BEHAVIOUR=ON"
when /tsan|thread/
  CB_CMAKE_EXTRAS << "-DENABLE_SANITIZER_THREAD=ON"
when /msan|memory/
  CB_CMAKE_EXTRAS << "-DENABLE_SANITIZER_MEMORY=ON"
end

BUILD_DIR = if CB_SANITIZER.empty?
              File.join(PROJECT_ROOT, "cmake-build-tests")
            else
              File.join(PROJECT_ROOT, "cmake-build-tests-#{CB_SANITIZER}")
            end

FileUtils.rm_rf(BUILD_DIR, verbose: true)
FileUtils.mkdir_p(BUILD_DIR, verbose: true)

Dir.chdir(BUILD_DIR) do
  if RUBY_PLATFORM =~ /mswin|mingw/
    CB_CMAKE_EXTRAS << "-DOPENSSL_ROOT_DIR=C:/Program Files/OpenSSL-Win64"
  else
    CB_CMAKE_EXTRAS << "-DCMAKE_C_COMPILER=#{CB_CC}" << "-DCMAKE_CXX_COMPILER=#{CB_CXX}"
  end
  run(CB_CMAKE,
      "-DCMAKE_BUILD_TYPE=#{CB_CMAKE_BUILD_TYPE}",
      "-DCOUCHBASE_CXX_CLIENT_BUILD_DOCS=OFF",
      "-DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=ON",
      "-DCACHE_OPTION=#{CB_CACHE_OPTION}",
      *CB_CMAKE_EXTRAS,
      "-B", BUILD_DIR,
      "-S", PROJECT_ROOT)

  run(CB_CMAKE,
      "--build", BUILD_DIR,
      "--parallel", CB_NUMBER_OF_JOBS,
      "--config", CB_CMAKE_BUILD_TYPE,
      "--verbose")
end

if RUBY_PLATFORM =~ /mswin|mingw/
  cbc = Dir["#{BUILD_DIR}/**/cbc.exe"].first
  if File.exist?(cbc)
    run("#{cbc} version --json")
  end
else
  run("#{BUILD_DIR}/tools/cbc version --json")
end
