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

pcall(require,"luarocks.require")

function isDir(path)
	local lfs = require 'lfs'
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

local function createConnection(sourceName, ...)
	require"luasql.sqlite3"
	local fileName = (options.PERSISTENCE_PATH or '') .. (sourceName or sourceName or options.PERSISTENCE_FILENAME)
	mkdir(dirName(fileName))
	local env = luasql.sqlite3()
	print(fileName)
	connection = assert(env:connect(fileName,...))
end


-----------------------------------------------------------------
-- <SQL API> ----------------------------------------------------
-----------------------------------------------------------------

local sql = {}

--- directly executes an SQL statement using simple parameters
--  @params	sql	sql instruction to execute. Must have proper placeholders for string.format
--	@params ...	paremeters to be used in string.format
--	@return query result
function sql.exec(sql, ...)
	local s = string.format(sql, ...)
	--print(s)
	return assert(connection:execute(s)) 
end

function sql.exists(tableName, id)
	local cursor = sql.exec('select id from %s where id = %d', tableName, id) 
	local row = {} 
	row = cursor:fetch(row)
	cursor:close()
	return row and row[1] or row
end

function sql.select(tableName, id, filters)
	local filteredById
	local renderedAttribs
	local filters = filters or {}
	table.foreach(filters, function(field, value)
		if string.sub(field, 1, 1) ~= '_' then

			renderedAttribs = string.format("%s%s%s='%s'", 
				renderedAttribs or '', 
				renderedAttribs and ' AND ' or '',
				field, value )
			
		end
	end)

	
	if tonumber(id) then
		id = {id}
	end 

	if type(id)=='table' then
		local ids 
		
		table.foreach(id, function(_,item)
			ids = string.format("%s%s%d", 
				ids or '',
				ids and ', ' or '',
				tonumber(item))
		end)
		filteredById = string.format('ID in (%s)', ids)
	end
	
	local where = (renderedAttribs or filteredById) and 'WHERE '
	local sql = string.format('select * from %s %s %s', tableName, where or '', filteredById or renderedAttribs or '')

	local cursor = assert(connection:execute(sql))
	local row={}
	local list={}
	while row do
		row = cursor:fetch({}, '*a')
		table.insert(list, row) 
	end
	cursor:close()
	return list
end

function sql.insert(tableName, data)
	local renderedFields
	local renderedValues
	
	table.foreach(data, function(field, value)
		if string.sub(field, 1, 1) ~= '_' then
			renderedFields = string.format("%s%s%s", 
				renderedFields or '', 
				renderedFields and ', ' or '',
				field)
	
			renderedValues = string.format("%s%s'%s'", 
				renderedValues or '', 
				renderedValues and ', ' or '',
				value)
		end
	end)
	

	local ok, msg = sql.exec('insert into %s (%s) values (%s)', tableName, renderedFields, renderedValues)
	return ok, msg or (ok and 'Registro inserido com sucesso')
end

function sql.update(tableName, id, data)
	local renderedAttribs
	local filteredById
	
	table.foreach(data, function(field, value)
		if string.sub(field, 1, 1) ~= '_' then
			renderedAttribs = string.format("%s%s%s='%s'", 
				renderedAttribs or '', 
				renderedAttribs and ', ' or '',
				field, value )
		end
	end)

	if tonumber(id) then
		id = { tonumber(id) }
	end
	
	if type(id)=='table' then
		local ids 
		table.foreach(id, function(_,item)
			ids = string.format("%s%s%d", 
				ids or '',
				ids and ', ' or '',
				tonumber(item))
		end)
		filteredById = string.format('WHERE id in (%s)', ids)
	end

	local ok, msg = sql.exec('update %s set %s %s', tableName, renderedAttribs, filteredById or '')
	return ok, msg or (ok and tostring(ok) .. ' Registro(s) atualizado(s) com sucesso')
end

function sql.delete(tableName, id, data)
	local filteredById
	
	if tonumber(id) then
		id = {id}
	end 

	if type(id)=='table' then
		local ids 
		
		table.foreach(id, function(_,item)
			ids = string.format("%s%s%d", 
				ids or '',
				ids and ', ' or '',
				tonumber(item))
		end)
		filteredById = string.format('(%s)', ids)
	else
		return false, 'Precisa fornecer o ID ou a lista de IDs para deleção'
	end
	
	local ok, msg = sql.exec('delete from %s WHERE id in %s', tableName, filteredById)
	
	return ok, msg or (ok and tostring(ok) .. ' Registro(s) deletados(s) com sucesso')
end


function sql.createTable(tableName, structure)
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
	sql.exec(table.concat(sqlTable, '\n'))
end

function sql.existTable(tableName)
	return connection:execute(string.format("select NULL from %s", tableName))
end

-----------------------------------------------------------------
-- </SQL API> ---------------------------------------------------
-----------------------------------------------------------------

local function assertTableExists(tableName, structure)

	local res, msg = sql.existTable(tableName)
	
	if not res then
		if not options().AUTO_CREATE_TABLES then
			error(msg)
		end
		
		sql.createTable(tableName, structure)
		
	else
		return res
	end
end

local function getTypeName(class)

	if type(class)=='string' then
		return class
	elseif type(class)=='table' and rawget(class, '__typeName') then
		return rawget(class, '__typeName')
	else
		return 1
	end
end

local function getPhysicalTableName(class)
	-- TODO: add sophistication
	if type(class)=='table' and rawget(class, '__tableName') then
		return rawget(class, '__tableName')
	else
		return getTypeName(class)
	end
end


local function getPhysicalTableStructure(class)
	-- TODO: add sophistication
	if type(class)=='table' and rawget(class, '__tableName') then
		return rawget(class, '__tableName')
	else
		return getTypeName(class)
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
	-- if options are set to isolate IDs per type use typename
	local typeName = options.ISOLATE_IDS_PER_TYPE and class and getPhysicalTableName(class)
	typeName = typeName or 'Config'
	
	local idsLeft = aIdsLeft[typeName]
	local nextId = aNextId[typeName]
	
	-- if there's still IDs in the series at hand
	if idsLeft and idsLeft > 0 then
		aNextId[typeName] = nextId + 1
		aIdsLeft[typeName] = idsLeft - 1 
		return nextId
	end

	-- get a new series'
	
	local tableName = '_Id_' .. typeName
	
	assertTableExists(tableName, {
		lastId='INT'
	})
	
	connection:setautocommit(false)
	
	-- save latest id + size of the series into persistence
	local s = string.format([[update %s set lastId = lastId + %d + 1]], tableName, seriesSize)
	--print(s)
	sql.exec(s)
	
	-- update id series
	local cursor = sql.exec([[select lastId from %s]], tableName)
	
	assert(cursor)
	local lastIdOfSeries = cursor:fetch()
	cursor:close()
	
	-- if that table has never got any IDs
	if not lastIdOfSeries then
		lastIdOfSeries = seriesSize
		sql.insert(tableName, {lastId=seriesSize+1})
	end
	
	connection:commit()
	connection:setautocommit(true)
	
	local lastId = lastIdOfSeries - seriesSize + 1
	 
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

function persist(class, id, data)
	local tableName = getPhysicalTableName(class)
	
	assertTableExists(tableName, class)
	
	if sql.exists(tableName, id) then
		sql.update(tableName, id, data)
	else
		sql.insert(tableName, data)
	end
	
end


-- retrieve(class, id)
--- Obtains a table from the persistence that 
-- has the proper structure of an object of a given type
-- @param class the schema class identifying the type of the object to retrieve
-- @param id identifier of the object to load
function retrieve(class, id)
	local tableName = getPhysicalTableName(class)
	
	return sql.select(tableName, id)

end 

-- erase(class, id)
--- Eliminates a record  from the persistence that 
-- corresponds to the given id 
-- @param class the schema class identifying the type of the object to retrieve
-- @param id identifier of the object to remove
function erase(class, id)
	local tableName = getPhysicalTableName(class)
	
	return sql.delete(tableName, id)

end 


-- persistSimple(id, data)
-- retrieveSimple(id)

-- eraseSimple(id)

-- search(class, filter, visitorFunction)
--- Perform a visitor function on every record obtained  
-- in the persistence through a given set of filters
-- @param class the schema class identifying the type of the object to retrieve
-- @param filter 	table containing a set of filter conditions
--					
-- @param visitor	(optional) function to be executed 
-- 					every time an item is found in persistence
function search(class, id, visitorFunction)
	local tableName = getPhysicalTableName(class)
	
	rows =  sql.select(tableName, filter) or {}
	
	for _,data in pairs(rows) do
		visitorFunction(data)
	end
end
