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
  
local defaultOptions = {
	PERSISTENCE_PATH = './db/';
	PERSISTENCE_FILENAME = 'persistence.db3';
	ISOLATE_IDS_PER_TYPE = true;
	AUTO_CREATE_TABLES = true;
}



-----------------------------------------------
-- IMPLEMENTATION
-----------------------------------------------

local connection

local options

pcall(require,"luarocks.require")

--- naive implementation of SQL quoting
-- @param ...  parameters to be quoted
-- @return all parameters quoted
function sqlQuote(...)
	local parameters = {...}
	table.foreach(parameters, function(_, item)
		if type(item)=='string' then
			parameters[_] = "'" .. string.gsub(item, "'", "''") .. "'"
		elseif tonumber(item)=='number' then
			parameters[_] = tostring(item)
		elseif type(item)=='nil' then
			parameters[_] = 'NULL'
		end
	end)
	return unpack(parameters)
end

--- directly executes an SQL statement using simple parameters
--  @params	sql	sql instruction to execute. Must have proper placeholders for string.format
--	@params ...	paremeters to be used in string.format
--	@return query result
local function doSQL(sql, ...)
	local s = string.format(sql, ...)
	--print(s)
	return assert(connection:execute(s)) 
end

local function createTable(tableName, structure)
	local sqlTable=setmetatable({},{__index=table}) 
	
	sqlTable:insert[[create table if not exists]]
	sqlTable:insert(tableName)
	sqlTable:insert[[(]]
	local i
	table.foreach(structure, function(field, type)
	sqlTable:insert(field .. '  ' .. (type or '') ..  ',')
	i = #sqlTable
	end)
	if i then
		sqlTable[i]=string.sub(sqlTable[i], 1, -2)
	end
	sqlTable:insert[[)]]
	doSQL(table.concat(sqlTable, '\n'))
end

local function assertTableExists(tableName, structure)

	local res, msg = connection:execute(string.format("select null from %s", tableName))
	
	if res then
		return res
	end
	
	if not options().AUTO_CREATE_TABLES then
		error(msg)
	else
		createTable(tableName, structure)
	end
end

------------------------------------------------------
-- API
------------------------------------------------------


-- initialize([ [sourceName], ...])
--- initializes the provider
function initialize(sourceName, ...)
	require"luasql.sqlite3"
	
	local env = luasql.sqlite3()
	connection = assert(env:connect(sourceName,...))
	connection:setautocommit(false)
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
	typeName = typeName or 'config'
	
	local idsLeft = aIdsLeft[typeName]
	local nextId = aNextId[typeName]
	
	-- if there's still IDs in the series at hand
	if idsLeft and aIdsLeft[typeName] > 0 then
		aNextId[typeName] = nextId + 1
		aIdsLeft[typeName] = idsLeft - 1 
		return nextId
	end

	-- get a new series'
	
	local tableName = 'Id' .. typeName
	
	assertTableExists(tableName, {
		lastId='INT'
	})
	
	-- save latest id + size of the series into persistence
	doSql([[update %s set lastId = lastId + %d + 1]], tableName, seriesSize)
	
	-- update memory id series
	local cursor = doSql([[select lastId from %s]], tableName)
	
	assert(cursor)
	local lastIdOfSeries = cursor:fetch()
	
	lastId =  lastIdOfSeries - seriesSize
	
	connection:commit()
	
	aNextId[typeName] = lastId + 1 
	aIdsLeft[typeName] = seriesSize 
	
	return lastId
end

end)()

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

-- persist(class, id, data)
-- retrieve(class, id)

-- erase(class, id)

-- persistSimple(id, data)
-- retrieveSimple(id)

-- eraseSimple(id)

-- search(class, filter, visitorFunction)