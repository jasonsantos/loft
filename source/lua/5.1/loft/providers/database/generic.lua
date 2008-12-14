local sql = require 'loft.providers.sql.generic'

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

-- initialize(appName)
-- reopen()
-- finalize()

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

Private = {}

Private.defaultOptions = {
	ISOLATE_IDS_PER_TYPE = true;
	AUTO_CREATE_TABLES = true;
}

-----------------------------------------------
-- IMPLEMENTATION
-----------------------------------------------

Private.environment = nil
Private.connection = nil
Private.connectionData = nil

local function openConnection()
	if not Private.connection then
		Private.connection = assert(Private.environment:connect(unpack(Private.connectionData)))
		sql.initialize(Private.connection, Private.connectionData)
	end
end

function createConnection(sourceName, ...)
end

local function closeConnection()
	if Private.connection then
		Private.connection:close()
		Private.connection = nil
	end
end

local function getTypeName(class)

	if type(class)=='string' then
		return class
	elseif type(class)=='table' and rawget(class, '.typeName') then
		return rawget(class, '.typeName')
	else
		return 1
	end
end

local function getPhysicalTableStructure(class, data)
	-- TODO: add sophistication
	local structure = { id = 'INT' }
	if type(class)=='table' and rawget(class, '.tableName') then
		if class.fields then --TODO: no schemas
			local names = class.fields.names() --TODO: no schemas
			local types = class.fields.types() --TODO: no schemas
			for ix, name in pairs(names or {}) do
				structure[name] = types[ix]
			end 
		end
	else
		for key, value in pairs(data or {}) do
			if key == 'id' then
				structure[key] = 'INT'
			elseif type(value) == 'number' then
				structure[key] = 'REAL'
			elseif type(value) == 'string' then
				structure[key] = 'TEXT'
			elseif type(value) == 'table' and value.id then
				structure[key] = 'INT'
			end
		end
	end
	return structure
end


local function getPhysicalTableName(class)
	-- TODO: add sophistication
	if type(class)=='table' and rawget(class, '.tableName') then
		return rawget(class, '.tableName')
	else
		return getTypeName(class)
	end
end

local function assertTableExists(tableName, class, data)

	local res, msg = sql.existTable(tableName)
	
	if not res then
		if not options().AUTO_CREATE_TABLES then
			error(msg)
		end
		
		local structure = getPhysicalTableStructure(class, data)
		
		sql.createTable(tableName, structure)
		
	else
		return res
	end
end


------------------------------------------------------
-- API
------------------------------------------------------


-- initialize([ [sourceName], ...])
--- initializes the provider
function initialize(sourceName, ...)
	return createConnection(sourceName, ...)
end


-- finalize()
--- finalizes the provider
function finalize()
	return closeConnection()
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
	
	
	return function(class)
		local p = function() end
		-- if options are set to isolate IDs per type use typename
		local typeName = options.ISOLATE_IDS_PER_TYPE and class and getPhysicalTableName(class)
		typeName = typeName or 'Config'
		
		local idsLeft = aIdsLeft[typeName]
		local nextId = aNextId[typeName]
	p('idsLeft',idsLeft)
		-- if there's still IDs in the series at hand
		if idsLeft and idsLeft > 0 then
			aNextId[typeName] = nextId + 1
			aIdsLeft[typeName] = idsLeft - 1 
			return nextId
		end
	
		-- get a new series'
		
		local tableName = '_id_' .. typeName
		
		openConnection()
		
		assertTableExists(tableName, nil, {
			lastId=0
		})
		
		Private.connection:setautocommit(false)
		
		-- save latest id + size of the series into persistence
		local s = string.format([[update %s set lastId = lastId + %d]], tableName, seriesSize)
		--print(s)
		sql.exec(s)
		
		-- update id series
		local cursor, err = sql.exec([[select lastId from %s]], tableName)
		if (cursor) then
		
			local lastIdOfSeries = cursor:fetch()
			cursor:close()
			
			-- if that table has never got any IDs
			if not lastIdOfSeries then
				lastIdOfSeries = seriesSize
				sql.insert(tableName, {lastId=seriesSize})
			end
			
			Private.connection:commit()
			Private.connection:setautocommit(true)
			
			closeConnection()
			
			p('lastIdOfSeries',lastIdOfSeries)
			local lastId = lastIdOfSeries - seriesSize
			p('lastId',lastId)
			 
			aNextId[typeName] = lastId + 1
			p('aNextId[typeName]',aNextId[typeName])
			aIdsLeft[typeName] = seriesSize - 1
			
			return lastId
		else
			return nil, err
		end
	end

end)() -- creates and returns the closure


-- options([optionTable])
--- every provider must have a public 'options' 
-- table. It must implement a metatable to allow
-- this table to be called as a function

-- @param optionTable 	when called with a table parameter, 
--				 		it sets all parameters present in the
--						table with new values  
options = setmetatable({}, {
	__index = Private.defaultOptions;
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

-- persist(class, id, data)
function persist(class, id, data)
	local tableName = getPhysicalTableName(class)
	
	openConnection()
	
	assertTableExists(tableName, class, data)
	
	if sql.exists(tableName, id) then
		sql.update(tableName, id, data)
	else
		sql.insert(tableName, data)
	end
	
	closeConnection()
	
	
end


-- retrieve(class, id)
--- Obtains a table from the persistence that 
-- has the proper structure of an object of a given type
-- @param class the schema class identifying the type of the object to retrieve
-- @param id identifier of the object to load
-- @return object of the given type corresponding to Id or nil
function retrieve(class, id)
	local tableName = getPhysicalTableName(class)
	
	openConnection()	
	local list = sql.select(tableName, id)
	closeConnection()	
	
	if list then
		return list[1]
	else
		return nil, 'not found'
	end

end 

-- erase(class, id)
--- Eliminates a record  from the persistence that 
-- corresponds to the given id 
-- @param class the schema class identifying the type of the object to retrieve
-- @param id identifier of the object to remove
function erase(class, id)
	local tableName = getPhysicalTableName(class)
	
	openConnection()
	local result = sql.delete(tableName, id)
	closeConnection()
	return result
end 


-- persistSimple(id, data)
function persistSimple(id, data)
	local tableName = '__sandbox'
	openConnection()
	assertTableExists(tableName, nil, {
		id=0,
		serialization='TEXT'
	})
	return persist(tableName, id, serialize.encode(data)) -- TODO: serialize.encode in utils
end 


-- retrieveSimple(id)
function retrieveSimple(id)
	local tableName = '__sandbox'
	
	local data = retrieve(tableName, id)
	return serialize.decode(data.serialization) -- TODO: serialize.decode in utils
end 


-- eraseSimple(id)
function eraseSimple(id)
	local tableName = '__sandbox'
	openConnection()
	local result = sql.delete(tableName, id)
	closeConnection()
	return result
end 

local relayFunction = function(...) return ... end

-- search(class, filter, visitorFunction)
--- Perform a visitor function on every record obtained  
-- in the persistence through a given set of filters
-- @param class the schema class identifying the type of the object to retrieve
-- @param filter 	table containing a set of filter conditions
--					those filters can be improved by adding sort fields 
-- @param visitor	(optional) function to be executed 
-- 					every time an item is found in persistence
--					if ommited, function will return a list with everything it found
-- @return 			array with every return value of 
function search(class, filter, visitorFunction)
	local tableName = getPhysicalTableName(class)
	local list = {}
	
	openConnection()
	if not sql.existTable(tableName) then
		return list
	end
		
	rows =  sql.select(tableName, nil, filter or {}) or {}
	closeConnection()
	
	visitorFunction = visitorFunction or relayFunction
	for _,data in pairs(rows or {}) do
		--TODO: find a way of passing the list into the visitor, to allow custom sorting
		local o = visitorFunction(data)
		if o then
			table.insert(list, o)
		end
	end
	
	return list
end
