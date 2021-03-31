dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local exitgrab = false
local exit_url = false

local discovered = {}

local bad_items = {}

local current_access_token = nil
local current_broadcast_id = nil

if not urlparse or not http then
  io.stdout:write("socket not correctly installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local ids = {}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(abort)
  abort = true
  if abort then
    abortgrab = true
  end
  exitgrab = true
  if not bad_items[item_name] then
    io.stdout:write("Aborting item " .. item_name .. ".\n")
    io.stdout:flush()
    bad_items[item_name] = true
  end
end

register_identifier = function(identifier)
  if identifier == item_value then
    return nil
  end
  for _, s in pairs({"id:" .. identifier, identifier .. ";" .. item_value}) do
    io.stdout:write("Registering " .. s .. " as discovered.\n")
    io.stdout:flush()
    local body, code, headers, status = http.request(
      "http://blackbird-amqp.meo.ws:23038/periscope-archived-pzp644wwf6omdpc/",
      s
    )
    io.stdout:write("Got status code " .. code .. ".\n")
    io.stdout:flush()
    if code ~= 200 and code ~= 409 then
      io.stdout:write("Could not set " .. s .. " as discovered.\n")
      io.stdout:flush()
      abort_item()
    end
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if not tested[s] then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  if parenturl
    and string.find(parenturl, "/api/v2/getUserBroadcastsPublic")
    and string.find(url, "/replay_thumbnail/") then
    return false
  end

  for s in string.gmatch(url, "([0-9a-zA-Z]+)") do
    if ids[s] then
      return true
    end
  end

  if string.match(url, "^https?://[^/]*video%.pscp%.tv/")
    or string.match(url, "^https?://[^/]+/api/")
    or string.match(url, "^https?://[^/]+/chatapi/v1/history")
    or string.match(url, "%.m3u8")
    or string.match(url, "%.ts")
    or (parenturl and string.match(url, "%.jpg")) then
    return true
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  if is_css then
    return urls
  end
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.gsub(url_, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])", function (s)
      local i = tonumber(s, 16)
      if i < 128 then
        return string.char(i)
      else
        -- should not have these
        abort_item()
      end
    end)
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    --url_ = string.match(url_, "^(.-)/?$")
    url_ = string.match(url_, "^(.-)\\?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^/>")
      or string.match(newurl, "^/&gt;")
      or string.match(newurl, "^/<")
      or string.match(newurl, "^/&lt;")
      or string.match(newurl, "^/%*") then
      return false
    end
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    checknewurl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function get_history(token, endpoint, cursor)
    local data = {
      url = endpoint .. "/chatapi/v1/history",
      access_token = token,
      cursor = "",
      limit = 1000,
      since = 0
    }
    if cursor then
      data["cursor"] = cursor
    end
    data = JSON:encode(data)
    if addedtolist[data] then
      return nil
    end
    addedtolist[endpoint .. data] = true
    print(data)
    table.insert(urls, {
      url = endpoint .. "/chatapi/v1/history",
      post_data = data
    })
  end

  local function jg(json, location) -- json_get
    for _, s in pairs(location) do
      if not json or not json[s] then
        io.stdout:write("Could not find key.\n")
        io.stdout:flush()
        abort_item()
        return false
      end
      json = json[s]
    end
    return json
  end

  if allowed(url, nil) and status_code == 200
    and not string.match(url, "%.jpg")
    and not string.match(url, "%.ts") then
    html = read_file(file)
    if string.match(url, "^https?://pscp%.tv/[^/]+/")
      or string.match(url, "^https?://www%.pscp%.tv/[^/]+/")
      or string.match(url, "^https?://periscope%.tv/[^/]+/")
      or string.match(url, "^https?://www%.periscope%.tv/[^/]+/")
      or string.match(url, "/card$") then
      local match = string.match(html, 'data%-store="({.-})"')
      if not match then
        abort_item()
      end
      local api_url = nil
      if string.match(url, "^https?://[^/]*pscp%.tv/") then
        api_url = "https://proxsee.pscp.tv"
      elseif string.match(url, "^https?://[^/]*periscope%.tv/") then
        api_url = "https://api.periscope.tv"
      end
      local json = JSON:decode(string.gsub(match, "&quot;", '"'))
      local identifier = nil
      for s, _ in pairs(jg(json, {"BroadcastCache", "broadcasts"})) do
        if identifier then
          io.stdout:write("Found bad number of broadcasts.\n")
          io.stdout:flush()
          abort_item()
        end
        identifier = s
        current_broadcast_id = identifier
      end
      if not identifier and string.match(url, "/card$") then
        if not current_broadcast_id then
          io.stdout:write("Broadcast identifier not found yet.\n")
          io.stdout:flush()
          abort_item()
        end
        identifier = current_broadcast_id
      end
      if not identifier then
        io.stdout:write("Found no broadcast.\n")
        io.stdout:flush()
        abort_item()
      end
      ids[identifier] = true
      register_identifier(identifier)
      check("https://pscp.tv/w/" .. identifier)
      check(api_url .. "/api/v2/accessVideoPublic?broadcast_id=" .. identifier .. "&replay_redirect=false")
      check(
        api_url .. "/api/v2/publicReplayThumbnailPlaylist"
        .. "?broadcast_id=" .. identifier
        .. "&session_id=" .. jg(json, {"SessionToken", "public", "thumbnailPlaylist", "token", "session_id"})
      )
      if not string.match(url, "/card$") then
        local user_id = string.match(html, 'name="twitter:text:broadcaster_id"%s+content="([^"]+)"')
        if not user_id then
          io.stdout:write("Could not find user_id.\n")
          io.stdout:flush()
          abort_item()
        end
        check(
          api_url .. "/api/v2/getUserBroadcastsPublic"
          .. "?user_id=" .. user_id
          .. "&all=true"
          .. "&session_id=" .. jg(json, {"SessionToken", "public", "broadcastHistory", "token", "session_id"})
        )
      end
      local a, b = string.match(url, "^(https?://[^/]-)[^%.]+%.tv(/.+)$")
      if a and b then
        check(a .. "periscope.tv" .. b)
        check(a .. "pscp.tv" .. b)
      end
    end
    if string.match(url, "%.m3u8") then
      for line in string.gmatch(html, "([^\n]+)") do
        if not string.match(line, "^#") then
          checknewshorturl(line)
        end
      end
    end
    if string.match(url, "^https?://[^/]+/api/v2/accessVideoPublic") then
      local json = JSON:decode(html)
      local api_url = string.match(url, "^(https?://[^/]+)")
      check(api_url .. "/api/v2/accessChatPublic?chat_token=" .. jg(json, {"chat_token"}))
      check(api_url .. "/api/v2/replayViewedPublic?life_cycle_token=" .. jg(json, {"life_cycle_token"}) .. "&auto_play=false")
    end
    if string.match(url, "^https?://[^/]+/api/v2/replayViewedPublic") then
      local json = JSON:decode(html)
      local api_url = string.match(url, "^(https?://[^/]+)")
      check(api_url .. "/api/v2/pingReplayViewedPublic?session=" .. jg(json, {"session"}))
    end
    if string.match(url, "^https?://[^/]+/api/v2/accessChatPublic") then
      local json = JSON:decode(html)
      current_access_token = jg(json, {"access_token"})
      if current_access_token ~= jg(json, {"replay_access_token"}) then
        io.stdout:write("Not a replay?\n")
        io.stdout:flush()
        abort_item()
      end
      get_history(current_access_token, jg(json, {"endpoint"}))
    end
    if string.match(url, "^https?://[^/]+/chatapi/v1/history") then
      if not current_access_token then
        abort_item()
      end
      local json = JSON:decode(html)
      local cursor = jg(json, {"cursor"})
      if string.len(cursor) > 0 then
        get_history(current_access_token, string.match(url, "^(https?://[^/]+)"), cursor)
      end
    end
    if string.match(url, "^https?://[^/]+/api/v2/getUserBroadcastsPublic") then
      local json = JSON:decode(html)
      for _, broadcast in pairs(jg(json, {"broadcasts"})) do
        if jg(broadcast, {"class_name"}) ~= "Broadcast" then
          io.stdout:write("Found a broadcast that is not a broadcast.\n")
          io.stdout:flush()
          abort_item()
        end
        discovered["id:" .. jg(broadcast, {"id"})] = true
      end
      local cursor = jg(json, {"cursor"})
      if string.len(cursor) > 0 then
        if not string.find(url, "&cursor=") then
          check(url .. "&cursor=" .. cursor)
        else
          check(string.gsub(url, "([%?&]cursor=)[^%?&]+", "%1" .. cursor))
        end
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ':%s*url%(([^%)"]+)%)') do
      checknewurl(newurl)
    end
  end

  return urls
end

set_new_item = function(url)
  local match = string.match(url, "^https?://www%.pscp%.tv/w/([0-9a-zA-Z]+)$")
  local type_ = "id"
  if match and not ids[match] then
    abortgrab = false
    exitgrab = false
    ids[match] = true
    item_value = match
    item_type = type_
    item_name = type_ .. ":" .. match
    io.stdout:write("Archiving item " .. item_name .. ".\n")
    io.stdout:flush()
  end
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if abortgrab or exitgrab then
    abort_item(true)
    return wget.actions.ABORT
    --return wget.actions.EXIT
  end

  set_new_item(url["url"])
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if exitgrab then
    return wget.actions.EXIT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] or addedtolist[newloc] then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if status_code == 429 then
    os.execute("sleep 14400")
    return wget.actions.ABORT
  end

  if status_code == 0
    or status_code >= 400 then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 1
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      if not allowed(url["url"], nil) then
        return wget.actions.EXIT
      end
      abort_item(true)
      return wget.actions.ABORT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local newitems = nil
  local file = io.open(item_dir .. '/' .. warc_file_base .. '_bad-items.txt', 'w')
  for item, _ in pairs(bad_items) do
    file:write(item .. "\n")
  end
  file:close()
  for item, _ in pairs(discovered) do
    io.stdout:write("Queuing item " .. item .. ".\n")
    io.stdout:flush()
    if newitems == nil then
      newitems = item
    else
      newitems = newitems .. "\0" .. item
    end
  end
  if newitems ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/periscope-discovered-pzp644wwf6omdpc/",
        newitems
      )
      if code == 200 or code == 409 then
        break
      end
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 10 then
      abort_item()
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    abort_item(true)
    return wget.exits.IO_FAIL
  end
  return exit_status
end

