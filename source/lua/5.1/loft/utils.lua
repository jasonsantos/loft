--- Useful functions that fit nowhere else

module(..., package.seeall)

--- execute a function using adding the table t to its environment
function with(t, f)
	local env = getfenv(2)
	table.foreach(t, function(k,v) env[k]=v end)
	setfenv(f, env)
	f()
	table.foreach(t, function(k,v) t[k] = env[k] end)
end


-- Serialization SubModule
serialization = {}

function serialization.tofile(file, obj)
	if type(file) == 'string' then
		
	end
	
	if type(file) ~= 'userdata' then
		error'invalid file'
	end
	_G ['__serializer__tmp__'] = obj or {}
	local s = save('__serializer__tmp__', _G ['__serializer__tmp__'])
	_G ['__serializer__tmp__'] = nil
	file:write(s .. "\n return __serializer__tmp__")
end

function serialization.decode(str)
	local f = loadstring(str)
	local resultObj    
	if f then
		resultObj = f() or {}
		g['__tmp__'] = nil
	end
	return resultObj
end

function serialization.encode(obj)
	_G ['__serializer__tmp__'] = obj or {}
	local s = save('__serializer__tmp__', _G ['__serializer__tmp__'])
	_G ['__serializer__tmp__'] = nil
	return s .. "\n return __serializer__tmp__"
end


-- ----------------------------------------------------------
-- serializer
-- See "Programming In Lua" chapter 12.1.2.
-- Also see forum thread:
--   http://www.gammon.com.au/forum/?id=4960
-- ----------------------------------------------------------

--[[

  Example of use:

  require "serialize"
  SetVariable ("mobs", serialize.save ("mobs"))  --> serialize mobs table
  loadstring (GetVariable ("mobs")) ()  --> restore mobs table 

  If you need to serialize two tables where subsequent ones refer to earlier ones
  you can supply your own "saved tables" variable, like this:

    require "serialize"
    result, t = serialize.save ("mobs")
    result = result .. "\n" .. serialize.save ("quests", nil, t)

  In this example the serializing of "quests" also knows about the "mobs" table
  and will use references to it where necessary.  

  You can also supply the actual variable if the variable to be serialized does
  not exist in the global namespace (for instance, if the variable is a local 
  variable to a function). eg.

     require "serialize"
     do
      local myvar = { 1, 2, 8, 9 }
      print (serialize.save ("myvar", myvar))
    end

  In this example, without supplying the location of "myvar" the serialize would fail
  because it would not be found in the _G namespace.

--]]

local save_item  -- forward declaration, function appears near the end

function save (what, v, saved)

  saved = saved or {} -- initial table of tables we have already done
  v = v or _G [what]  -- default to "what" in global namespace

  assert (type (what) == "string", 
          "1st argument to serialize.save should be the *name* of a variable")
  
  assert (v, "Variable '" .. what .. "' does not exist")

  assert (type (saved) == "table" or saved == nil, 
          "3rd argument to serialize.save should be a table or nil")

  local out = {}  -- output to this table
  save_item (what, v, out, 0, saved)   -- do serialization
  return table.concat (out, "\n"), saved  -- turn into a string (also return our table)
end -- serialize.save

--- below are local functions for this module -------------

local function basicSerialize (o)
  if type(o) == "number" or type(o) == "boolean" then
    return tostring(o)
  else   -- assume it is a string
    return string.format("%q", o)
  end
end -- basicSerialize 

--
-- Lua keywords might look OK to not be quoted as keys but must be.
-- So, we make a list of them.
--

local lua_reserved_words = {}

for _, v in ipairs ({
    "and", "break", "do", "else", "elseif", "end", "false", 
    "for", "function", "if", "in", "local", "nil", "not", "or", 
    "repeat", "return", "then", "true", "until", "while"
            }) do lua_reserved_words [v] = true end

-- ----------------------------------------------------------
-- save one variable (calls itself recursively)
-- 
-- Modified on 23 October 2005 to better handle keys (like table keys)
-- ----------------------------------------------------------
function save_item (name, value, out, indent, saved)  -- important! no "local" keyword
  local iname = string.rep (" ", indent) .. name -- indented name

  -- numbers, strings, and booleans can be simply serialized

  if type(value) == "number" or 
     type(value) == "string" or
     type(value) == "boolean" then
    table.insert (out, iname .. " = " .. basicSerialize(value))

  -- tables need to be constructed, unless we have already done it

  elseif type(value) == "table" then
    if saved[value] then    -- value already saved?
      table.insert (out, iname .. " = " .. saved[value])  -- use its previous name
    else

  -- remember we have created this table so we don't do it twice

      saved [value] = name   -- save name for next time

  -- make the table constructor, and recurse to save its contents
  

      assert (string.find (name, "^[_%a][_%a%d%.%[%]\"\"]*$") 
              and not lua_reserved_words [name], 
              "Invalid name '" .. name .. "' for table")

      
      table.insert (out, iname .. " = {}")   -- create a new table

      for k, v in pairs (value) do      -- save its fields
        local fieldname 

        -- if key is a Lua variable name which is not a reserved word
        -- we can express it as tablename.keyname

        if type (k) == "string"
           and string.find (k, "^[_%a][_%a%d]*$") 
           and not lua_reserved_words [k] then
          fieldname = string.format("%s.%s", name, k)

        -- if key is a table itself, and we know its name then we can use that
        --  eg. tablename [ tablekeyname ]

        elseif type (k) == "table" and saved[k] then
          fieldname = string.format("%s[%s]", name, saved [k]) 

        -- if key is an unknown table, we have to raise an error as we cannot
        -- deduce its name
 
        elseif type (k) == "table" then
          error ("Key table entry " .. tostring (k) .. 
                 " in table " .. name .. " is not known")

        -- if key is a number or a boolean it can simply go in brackets,
        -- like this:  tablename [5] or tablename [true]

        elseif type (k) == "number" or type (k) == "boolean" then
          fieldname = string.format("%s[%s]", name, tostring (k))

        -- now key should be a string, otherwise an error
 
        elseif type (k) ~= "string" then
          error ("Cannot serialize table keys of type '" .. type (k) ..
                 "' in table " .. name)

        -- if key is a non-variable name (eg. "++") then we have to put it
        -- in brackets and quote it, like this:  tablename ["keyname"]

        else
          fieldname  = string.format("%s[%s]", name,
                                        basicSerialize(k))  
        end

        -- now we have finally worked out a suitable name for the key,
        -- recurse to save the value associated with it

        save_item(fieldname, v, out, indent + 2, saved) 
      end
    end

  -- cannot serialize things like functions, threads

  else
    error ("Cannot serialize '" .. name .. "' (" .. type(value) .. ")")
  end
end  -- save_item 