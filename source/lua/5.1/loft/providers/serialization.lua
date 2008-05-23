----------------------------------------------
-- Persistence Provider for the Loft Module
----------------------------------------------
-- 
module(..., package.seeall)

-----------------------------------------------
-- API
-----------------------------------------------
-- All persistence providers must implement 
-- this API to respond to the Loft engine
-----------------------------------------------
-- supportSchemas();

-- options(optionsTable);

-- getNextId([typeName]);

-- persist(class, id, data)
-- retrieve(class, id)

-- erase(class, id)

-- persistSimple(id, data)
-- retrieveSimple(id)

-- eraseSimple(id)

-- search(class, filter, visitorFunction)


-----------------------------------------------
-- OPTIONS
-----------------------------------------------
---  this provider default options

-- Variables available for path and filename
-- ${fileType} 	-- type of the file being saved
--				   either: 'control', 'data' or 'index'
-- ${typeName}	-- name of type used to select the schema
-- ${series}	-- Id based series. 
--				   a series is the number of the ID / MAX_IDS_PER_FILE  
local defaultOptions = {
	PERSISTENCE_PATH = './db/${fileType}/${typeName}/';
	PERSISTENCE_FILENAME = '${series}.serialized';
	UPDATE_THRESHOLD = 6; -- seconds
	MAX_IDS_PER_FILE = 1000; -- seconds
	ISOLATE_IDS_PER_TYPE = true;
}

-- options([optionTable])
--- every provider must have a public 'options' 
-- table. It must implement a metatable to allow
-- this table to be called as a function

-- @param optionTable 	when called with a table parameter, 
--				 		it sets all parameters present in the
--						table with new values  
options = setmetatable({}, {
	__index = defaultOptions;
	__call = function(f, optionTable)
		if optionTable then
			assert(type(optionTable)=='table', 'options argument must be a table')
			table.foreach(optionTable, function(key, value)
				options[key] = value
			end)
		end
		return options 
	end;
})

-----------------------------------------------
-- IMPLEMENTATION
-----------------------------------------------

-- requires

pcall(require,"luarocks.require")
local lfs = require "lfs"


-----------------------------------------------
-- PHYSICAL PERSISTENCE
-----------------------------------------------

local function getTypeName(class)

	if type(class)=='string' then
		return class
	elseif type(class)=='table' and rawget(class, '__typeName') then
		return rawget(class, '__typeName')
	else
		return 1
	end
end

local function getCache(typeName)
	local typeName = typeName
	local g = getfenv(0)
	
	g['cache'] = g['cache'] or {}  
	g['cache'][typeName] = g['cache'][typeName] or {}
	
	return g['cache'][typeName]
end

local function getSeries(typeName, id)
	return math.floor( id / options.MAX_IDS_PER_FILE )
end


function isDir(path)
	return lfs.attributes(path, 'mode') == "directory"
end

function dirName (path)
  return string.match(path, "^(.*)/[^/]*$")
end
  
  
function mkdir(path)
	local parent = dirName (path)
	local result = true, msg 
	if not isDir(parent) then
		result, msg = mkdir(parent)
	end
	return result and lfs.mkdir(path), msg
end

local function getFileName(path, typeName, fileType, series)
	local variables = {
		typeName = typeName or 'default';
		fileType = fileType or 'control';
		series = series or 'data';
	}
	return string.gsub(path, "${([^}]-)}", function(key)
		return variables[key]
	end)
end

function lock(typeName, f)
	local path = options.PERSISTENCE_PATH .. options.PERSISTENCE_FILENAME;
	
	if lfs then
		local f = f
		if not f then
			local fileName = getFileName(path, typeName)
			mkdir(dirName(fileName))
			f = io.open(fileName, 'r+b')
		end
		return f and lfs.lock(f, 'w'), f
	else
		local fileName = getFileName(path, typeName, 'lock')
		local f = io.open(fileName, 'rb')		
		if f then
			f:close()
			return false
		else
			f = io.open(fileName, 'wb')
			if f then
				f:close()
				return true
			else
				return false
			end 
		end
	end 
end

function unlock(typeName, f)
	if lfs then
		lfs.unlock(f)
	else
		local path = options.PERSISTENCE_PATH .. options.PERSISTENCE_FILENAME;
		local fileName = getFileName(path, typeName, 'lock')
		os.remove(fileName)
	end
end

function lockControl(typeName)
	local path = options.PERSISTENCE_PATH .. options.PERSISTENCE_FILENAME;
	local fileName = getFileName(path, typeName)
	mkdir(dirName(fileName))
	
	local file = io.open(fileName, 'r+b')
	if not file then
		file = io.open(fileName, 'w+b')
	end
	file:close()
	
	local locked, filelock = lock(typeName)
	local tries = 10
	while tries > 0 and not lock do
		locked, filelock = lock(typeName)
		tries = tries - 1
	end
	
	if not filelock then
		error('cannot lock control file for ' .. typeName)
	end
	return locked, filelock
end

--TODO: solve this mess
local function getControlFile(typeName)
	local filePattern = options.PERSISTENCE_PATH .. options.PERSISTENCE_FILENAME; 
	local fileName = getFileName(filePattern, typeName) 
	mkdir(dirName(fileName))
	return io.open(fileName, 'w+b')
end

local function getControlFileReadOnly(typeName)
	local fileName = options.PERSISTENCE_PATH .. options.PERSISTENCE_FILENAME; 
	
	return io.open(getFileName(fileName, typeName), 'r+b')
end

local function getPersistenceFile(typeName, id)
	local filePattern = options.PERSISTENCE_PATH .. options.PERSISTENCE_FILENAME; 
	local series = getSeries(typeName, id) 
	local fileName = getFileName(filePattern, typeName, 'persistence', series)
	mkdir(dirName(fileName))
	return io.open(fileName, 'w+b')
end



------------------------------------------------------
-- API
------------------------------------------------------


-- initialize([ [sourceName], ...])
--- initializes the provider
function initialize(sourceName, ...)
	-- TODO:...
end

-- supportSchemas();
--- respond whether this provider supports schemas
-- @return true if schemas are supported
function supportSchemas()
	return package.loaded['schemas']
end


-- getNextId([typeName]);
--- returns the next ID to be used in an object.
-- must persists series of IDs to be able to respond
-- without the need to write immediately to persistent media.
-- optionally, some providers can have their IDs isoladed by typeName.
-- @param typeName	(optional) If the provider supports, it is possible
-- 					to obtain next ID for a specific type of object
-- @return ID of the next object to be created 

getNextId = (function ()

local aNextId = {}
local aIdsLeft = {}
local seriesSize = 20

local getCache = getCache

-- TODO: see that we do not lose one ID per series

return function(class)
	-- if options are set to isolate IDs per type use typename
	local typeName = options.ISOLATE_IDS_PER_TYPE and getTypeName(class)
	typeName = typeName or 1
	
	local idsLeft = aIdsLeft[typeName]
	local nextId = aNextId[typeName]
	
	-- if there's still IDs in the series at hand
	if idsLeft and aIdsLeft[typeName] > 0 then
		aNextId[typeName] = nextId + 1
		aIdsLeft[typeName] = idsLeft - 1 
		return nextId
	end

	-- get a new series'
	
	local cache = getCache(typeName)

	-- if already locked
	-- wait.. and try again.. to a limit.. then throw an error
	local locked, filelock = lockControl(typeName)
	
	assert(filelock, "can't get a lock")
	
	update(typeName)
	
	-- get latest id
	local lastId = cache.__control.lastId or 1
	
	-- save latest id + size of the series into persistence
	cache.__control.lastId = lastId + seriesSize + 1
	
	commit(typeName)
	
	unlock(typeName, filelock)
	
	-- update memory id series
	aNextId[typeName] = lastId + 1
	aIdsLeft[typeName] = seriesSize 
	
	return lastId
end

end)()




-- persist(class, id, data)
--- Saves to persistence a complex type instance
-- @param class the schema class identifying the type of the object to save
-- @param id identifier of the object to save
-- @param data lua table containing all data to be saved
function persist(class, id, data)
	local typeName = getTypeName(class)
	local cache = getCache(typeName)
	local series = getSeries(typeName, id)
	
	--[[ -- index logic -- must test it before adding to module 
		cache.__index = cache.__index or {}
		 
		table.foreach(data, function(key, value)
			cache.__index[key] = cache.__index[key] or {} 
			table.insert(cache.__index[key], id)
		end)
	]]
	
	cache.__series = cache.__series or {}
	cache.__series[series] = cache.__series[series] or {}
	cache.__series[series][id] = data  
	
	-- TODO: delegate this file processing to commit()
	-- TODO: implement transaction support through a memento pattern and a rollback function()
	
	local locked, filelock = lockControl(typeName)
	
	local file = getPersistenceFile(typeName, id)
	
	writeSerialized(file, cache.__series[series])
	-- TODO: get index file 
	-- local indexFile = getIndexFile(typeName, id) 
	-- writeSerialized(indexFile, cache.__index)
	
	file:close()
	
	unlock(typeName, filelock)
	
end

-- retrieve(class, id)
--- Obtains a table from the persistence that 
-- has the proper structure of an object of a given type
-- @param class the schema class identifying the type of the object to retrieve
-- @param id identifier of the object to load
function retrieve(class, id)
	local typeName = getTypeName(class)
	local cache = getCache(typeName)
	local series = getSeries(typeName, id)
	
	-- TODO: read from cache before loading files
	-- TODO: decide whether to load data files through use of control file
	
	local file = getPersistenceFile(typeName, id)
	
	-- load from disk the persistence data for that entity
	if file then
		local g = getfenv(0)
		local s = file:read("*a")
		print(s)
		local f = loadstring(s)    
		if f then
			cache.__series[series] = f() or {}
			print(g['__tmp__'])
			g['__tmp__'] = nil
		end
		file:close()
	end

	return cache.__series[series][id]

end 


-- TODO:...

-- ---------- -- ---------- -- ---------- -- ---------- 

local lastUpdate = {}

--- responds about whether the entity has been updated in the last few seconds
-- @param typeName (optional) the typename 
-- @return boolean indica 
local function isUpdated(typeName)
	local typeName = typeName or 1
	local UPDATE_THRESHOLD = options.UPDATE_THRESHOLD
	return (os.time() - (lastUpdate[typeName] or 0)) < UPDATE_THRESHOLD
end


--- loads the data from disk
function update(typeName)
	local typeName = typeName or 1
	local g = getfenv(0)
	
	g['cache'] = g['cache'] or {}  
	g['cache'][typeName] = g['cache'][typeName] or {}

	local controlFile = getControlFileReadOnly( typeName )
	
	-- load from disk the control data for that entity
	if controlFile then
		local s = controlFile:read("*a")
		--[[local f = loadstring(s)    
		if f then
			g['cache'][typeName]['__control'] = f() or {}
			g['__tmp__'] = nil
		end
		]]--
		g['cache'][typeName]['__control'] = utils.serialization.decode(s)
		
		controlFile:close()
	end
	
	lastUpdate[typeName] = os.time()
end



function commit(typeName)
	local typeName = typeName or 1
	
	local cache = getCache(typeName)

	local controlFile = getControlFile( typeName )
	if controlFile then
		utils.serialization.tofile(controlFile, cache['__control'])
		controlFile:close()
	end
end

