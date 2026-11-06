# frozen_string_literal: true

#  Copyright 2026-Present Couchbase, Inc.
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

require "cgi"
require "fileutils"
require "open3"

# The site in refs/site/pages, shared by bin/check-abi and bin/check-perf.
module PagesSite
  class Error < StandardError; end

  # Outside refs/heads, so clones and fetches do not download the site and GitHub lists no branch.
  REF = "refs/site/pages"
  LABELS = {"abi" => "ABI history", "perf" => "Performance history"}.freeze
  # The site commits are authored as the github-actions[bot] account. BOT_ID is that account's user
  # id (GET /users/github-actions[bot]). GitHub attributes a commit to an account by its noreply
  # address, <id>+<login>@users.noreply.github.com.
  BOT_NAME = "github-actions[bot]"
  BOT_ID = 41_898_282
  BOT_EMAIL = "#{BOT_ID}+#{BOT_NAME}@users.noreply.github.com"

  # The root page: a link to every top-level directory that holds an index.html.
  def self.render_index(dir)
    sections = Dir.children(dir).sort.select { |d| File.file?(File.join(dir, d, "index.html")) }
    items = sections.map { |d| "<li><a href=\"#{CGI.escapeHTML(d)}/\">#{CGI.escapeHTML(LABELS.fetch(d, d))}</a></li>" }
    <<~HTML
      <!doctype html>
      <html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
      <meta name="color-scheme" content="light dark">
      <title>couchbase-cxx-client</title></head>
      <body style="font: 15px/1.5 system-ui, sans-serif; max-width: 72rem; margin: 0 auto; padding: 1rem">
      <h1>couchbase-cxx-client</h1>
      <ul>#{items.join}</ul>
      </body></html>
    HTML
  end

  # Authenticates git over HTTPS through the environment, so the token appears in no command line.
  def self.git_auth_env(token)
    return {} unless token

    {"GIT_CONFIG_COUNT" => "1", "GIT_CONFIG_KEY_0" => "http.https://github.com/.extraheader",
     "GIT_CONFIG_VALUE_0" => "AUTHORIZATION: basic #{["x-access-token:#{token}"].pack('m0')}"}
  end

  def self.git(*args, env: {})
    out, status = Open3.capture2e(env, "git", *args)
    raise Error, "git #{args.first(3).join(' ')} failed:\n#{out}" unless status.success?

    out
  end

  # Checks out the head of REF in url into site and yields site to change it. Then renders the root
  # index, commits as github-actions[bot] and pushes. A failed push starts again from the head of
  # REF. The block therefore runs once per attempt and must give the same result each time. Returns
  # false when the block changed nothing, true when pushed.
  def self.publish(url:, token:, site:, message:, attempts: 5)
    attempts.times do |attempt|
      FileUtils.rm_rf(site)
      git("init", "--quiet", site)
      if system("git", "ls-remote", "--exit-code", url, REF, out: File::NULL)
        git("-C", site, "fetch", "--quiet", "--depth", "1", url, REF)
        git("-C", site, "checkout", "--quiet", "FETCH_HEAD")
      end
      yield site
      File.write(File.join(site, "index.html"), render_index(site))
      git("-C", site, "add", "--all")
      return false if system("git", "-C", site, "diff", "--cached", "--quiet")

      git("-C", site, "-c", "user.name=#{BOT_NAME}", "-c", "user.email=#{BOT_EMAIL}",
          "commit", "--quiet", "--message", message)
      begin
        git("-C", site, "push", url, "HEAD:#{REF}", env: git_auth_env(token))
        return true
      rescue Error => e
        warn "push to #{REF} failed (attempt #{attempt + 1} of #{attempts}):\n#{e.message}"
      end
    end
    raise Error, "could not push to #{REF}"
  end
end
