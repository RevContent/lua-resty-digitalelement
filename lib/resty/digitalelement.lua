local bit         = require "bit"
local cjson       = require "cjson"
local iputils     = require "resty.iputils"
local resty_sha1  = require "resty.sha1"
local str         = require "resty.string"

local rsp = {} -- Responses
local lst = {} -- Lists
local _M  = {} -- Local Application

-- Check for a nested variable in a table, taken from
-- https://stackoverflow.com/a/53135271 which is easiest
-- way to validate that a JSON key exists.
function lookup(t, ...) 

  if not t then
    return nil
  end

  for _, k in ipairs{...} do
  
    t = t[k]
    if not t then
      return nil
    end
  
  end
  return t

end

-- Sort JSON keys, verifying consistency between
-- response payloads and preventing unnecessary
-- data from being stored.
function returnTableValuesSorted(tbl)

  local v = ""
  local ordered_keys = {}

  for k in pairs(tbl) do
      table.insert(ordered_keys, k)
  end

  table.sort(ordered_keys)
  for i = 1, #ordered_keys do
    v = v .. tbl[ordered_keys[i]]
  end
  return v

end

-- Create the trie to store the IP addresses
-- by octet.
function createTrie(key, ...)

  local t = lst[key]

  for _, k in ipairs{...} do

    -- If key doesn't exist yet, create it.
    if not t[k] then t[k] = {} end

    t = t[k]
  
  end

  return

end

-- Load databases into memory.
function init_file(key, db)

  -- Debug Metrics
  local ips = 0

  -- Setup Keypair, that way we will always
  -- be able to respond even if there is an
  -- error loading the file. 
  lst[key] = {}
  rsp[key] = {}

  -- Validate we can read the file
  local f = io.open(db.file, "r")
  if f == nil then
    return nil, "Unable to open file " .. db.file .. "."
  end
  io.close(f)

  -- Read file line by line
  for l in io.lines(db.file) do

    -- Decode JSON, ignore lines with error
    local valid, entry = pcall(cjson.decode, l)

    if valid then

      -- Turn the start and end IP into decimals.
      local s_ip, _ = iputils.ip2bin(entry["start-ip"])
      local e_ip, _ = iputils.ip2bin(entry["end-ip"])

      -- Remove unnecessary keys from response payload.
      entry["start-ip"] = nil
      entry["end-ip"]   = nil

      -- If response payload does not exist yet, create it. We return table
      -- values in a sorted array, then hash the value of that to get a unique
      -- representation of an profile response.
      local sha1 = resty_sha1:new()
      sha1:update(returnTableValuesSorted(entry))
      local digest = sha1:final()
      if not rsp[key][digest] then
        rsp[key][digest] = entry
      end

      -- Have loop skip to value if we were able
      -- to do a bulk insert.
      skip_to = 0

      -- Iterate over IP range
      for i = s_ip, e_ip
      do

        -- Have we already processed this in a bulk insert?
        if i < skip_to then goto continue end

        -- Turn int back into IP notation
        local n1 = bit.band(bit.rshift(i, 24), 255)
        local n2 = bit.band(bit.rshift(i, 16), 255)
        local n3 = bit.band(bit.rshift(i, 8), 255)
        local n4 = bit.band(bit.rshift(i, 0), 255)

        -- print("IP: " .. n1 .. "." .. n2 .. "." .. n3 .. "." .. n4)

        -- If we are at XXX.XXX.XXX.0, and there are 255 or more results left
        -- to iterate over, we know that the full octet is for a single profile.
        -- Instead of creating an array, we'll just point directly to that profile
        -- from n3.
        if n4 == 0 and (e_ip - i) >= 255 then

          createTrie(key, n1, n2)
          lst[key][n1][n2][n3] = digest
          
          skip_to = i + 256
          ips = ips + 255

        -- Create simple Trie with n3 being an array, and n4 being the index with a
        -- value of the profile array location.
        else

          -- Create/Validate Trie
          createTrie(key, n1, n2, n3)
          table.insert(lst[key][n1][n2][n3], n4, digest)

          -- Metrics
          ips = ips + 1

        end

        ::continue::
      
      end

    end

  end

  return ips, nil

end

-- Initialize Package
function _M.init(i)

  local errors = {}

  -- Iterate over each database input, load into memory.
  for k, v in pairs(i) do

    ngx.log(ngx.INFO, "Initializing " .. k .. " database from file " .. v.file ..".")

    ips, err = init_file(k, v)

    if err ~= nil then
      ngx.log(ngx.ERR, "Initialization Error: " .. err)
    else
      ngx.log(ngx.INFO, "Initialization Success, " .. ips .. " IPs loaded.")
    end
    
  end

  -- Reserving [1] return location for future updates
  -- and to preserve backward compatability.
  return nil, errors

end

-- Check to see if the IP is loaded into the trie.
function _M.lookup(key, ip)

  local err   = nil
  local iparr = {}

  -- Break down IP into each individual octet, load into
  -- a table for comparision against trie.
  for i in string.gmatch(ip, "%d+") do 
    table.insert(iparr, i)
  end

  -- Do we have 4 octets
  if table.getn(iparr) ~= 4 then
    return nil, "Invalid IP: " .. ip
  end

  -- Convert each octet to integer, validate between
  -- 0 and 255.
  for i=1, 4
  do
    iparr[i] = tonumber(iparr[i])
    if iparr[i] < 0 or 255 < iparr[i] then
      return nil, "Invalid IP: " .. ip
    end
  end

  -- Check to see if value exists in Trie
  local profile = lookup(lst[key], iparr[1], iparr[2], iparr[3])
  if profile == nil then
  -- Nothing Found
    goto noip

  elseif type(profile) == "string" then
  -- Whole /24 matches to a single profile
    return rsp[key][profile], nil

  else
  -- Key has _something_ in it, check to see if our IP is in it.
    local i = lst[key][iparr[1]][iparr[2]][iparr[3]][iparr[4]]
    if i ~= nil then
      return rsp[key][i], nil
    end
  end

  -- No IPs found
  ::noip::
  return nil, "Unable to locate IP."

end

return _M