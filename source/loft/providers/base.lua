local cosmo = require 'cosmo'

require'util'
require'events'

----------------------------------------------
-- Persistence Provider for the Loft Module
----------------------------------------------
-- 

module(..., package.seeall)

description = [[Generic Module for Database Behaviour]]

-- ######################################### --
-- # API
-- ######################################### --
--  All persistence providers must implement 
--  this API to respond to the Loft engine
--  the 'base' provider will provide all of these 
--  interfaces, and you can extended them
--  in your own providers 
-----------------------------------------------

-- -------------- --
-- PROPERTIES
-- -------------- --

-- quotes
-- filters.like
-- filters.contains
-- escapes.quotes
-- escapes.new_lines
-- escapes.reserved_field_name

-- database_type 
-- reserved_words
-- field_types
-- sql

-- -------------- --
-- FUNCTIONS
-- -------------- --

-- setup(engine)

-- create(engine, entity)
-- persist(engine, entity, id, data)
-- retrieve(engine, entity, id)
-- delete(engine, entity, id)

-- search(engine, options)
-- count(engine, options)



-- ######################################### --
--  OPTIONS
-- ######################################### --
--  this provider default options

local indexed_table_mt = {
	__newindex=function(t,k,v)
		if not tonumber(k) then
			table.insert(t, k)
		end
		rawset(t,k,v)
	end,
	__call=function(t,items)
		for _,v in ipairs(items) do
			t[v]=true
		end
	end
}

database_type = 'base'

reserved_words = setmetatable({}, indexed_table_mt)
 
reserved_words {
	'and',
	'fulltext', 
	'table'
}
 
filters = {
	
	like = function(s) 
		return string.gsub(s or '', '[*]', '%%')
	end,
	
	contains =function(f,s) 
		return string.format("CONTAINS(%s, %s)", f, s)
	end,

}

quotes = [[']]

escapes = {
	quotes = function(s) 
		return string.gsub(s or '', "'", "''")
	end,
	
	new_lines = function(s) 
		return string.gsub(s or '', "\n", "\\n")
	end,
	
	reserved_field_name =function(s)
		return string.format('`%s`', s) 
	end
	
}

---ToDo: Checar com o jason, a nova fun��o do escape_field_name
local function contains_special_chars(s)
	return string.find(s, '([^a-zA-Z0-9_])')~=nil
end

escape_field_name=function(s)
	return (reserved_words[string.lower(s)] or contains_special_chars(s)) and escapes.reserved_field_name(s) or s
end

string_literal=function(s) 
	return quotes .. escapes.quotes(escapes.new_lines(s)) .. quotes
end

field_types = {
	key={type='BIGINT', size='8', required=true, primary=true, autoincrement=true, onEscape=tonumber},
	integer={type='INT', size='5', onEscape=tonumber},
	number={type='DOUBLE', onEscape=tonumber},
	currency={type='DECIMAL', size={14,2}, onEscape=tonumber},
	text={type='VARCHAR',size='255', onEscape=string_literal},
	long_text={type='LONGTEXT', onEscape=string_literal},
	timestamp={type='DATETIME'}, 
	boolean={type='BOOLEAN'},
}

--  ]=]

sql = {
	
	CREATE = [==[
CREATE TABLE IF NOT EXISTS $table_name ( 
  $columns{", "}[=[$escape_field_name{$column_name} $type$if{$size}[[($size)]]$if{$primary}[[ PRIMARY KEY]]$if{$required}[[ NOT NULL]]$if{$description}[[ COMMENT  $string_literal{$description}]]$if{$autoincrement}[[ AUTO_INCREMENT]]$sep
]=])
]==],
	
	INSERT = [[INSERT INTO $table_name ($data{", "}[=[$escape_field_name{$column_name}$sep]=]) VALUES ($data{", "}[=[$value$sep ]=])]],
	
	UPDATE = [==[UPDATE $table_name SET $data{", "}[=[$escape_field_name{$column_name}=$value$sep]=] $if{$filters}[=[WHERE ($filters_concat{" AND "}[[$it$sep]])]=]]==],
	
	SELECT = [==[SELECT 
  $columns{", "}[[ $if{$column_name}[[$escape_field_name{$column_name}]][[$func]] as $escape_field_name{$alias}$sep
  ]]FROM $table_name
  $if{$filters}[=[WHERE ($filters_concat{" AND "}[[$it$sep]])]=] $if{$sorting}[=[ORDER BY $sorting_concat{", "}[[$it$sep]]]=] $if{$pagination}[=[$if{$pagination|limit}[[ LIMIT $pagination|limit ]] $if{$pagination|offset}[[OFFSET $pagination|offset]]]=]]==],
	
	DELETE = [==[DELETE FROM $table_name $if{$filters}[=[WHERE ($filters_concat{" AND "}[[$it$sep]])]=]]==],
	
	LASTID = [==[SELECT LAST_INSERT_ID()]==],
	
	GET_TABLES = [==[SHOW TABLES]==],
	
	GET_DESCRIPTION = [==[DESCRIBE $table_name]==]
	
}

-- ######################################### --
--  INTERNALS
-- ######################################### --


local passover_function = function(...) return ... end

local field_type = function(field)
	local field_type_name = field.type
	return field_types[field_type_name] or {}
end

local table_fill_cosmo = function (engine, entity)
	local t ={}
	
	t.table_name = entity['table_name'] or entity['name']
	
	if ( not t.table_name ) then
		error("Entity must have a  `name` or `table_name`.")
	end
	
	local columns = {}
	
	for field_name, field in pairs(entity.fields) do
		local field_type = field_types[field.type] or {}
		local column = table.merge(field_type, field)
		
		column.name = field.column_name
		column.alias = field_name
		column.order = field.order or 999
		column.type = field_type.type
		columns[field_name] = column 
		
		table.insert(columns, columns[field_name])
	end
	table.sort(columns, function(f1,f2) return f1.name < f2.name end)
	table.sort(columns, function(f1,f2) return f1.order < f2.order end)
	
	t.__columns = columns
	t.columns = cosmo.make_concat( columns )
	
	t['string_literal'] = function (arg) 
		return string_literal(arg[1])
	end
	
	t['escape_field_name'] = function (arg) 
		return escape_field_name(arg[1])
	end
	
	t["if"] = function (arg)
	   if arg[1] then arg._template = 1 else arg._template = 2 end
	   cosmo.yield(arg)
	end
	
	return t
end

local filters_fill_cosmo = function (table_fill_cosmo, _filters)
	local t = table_fill_cosmo
	if ( type(_filters) == "table" and next(_filters)) then
		local lines = {}
		
		for name, val in pairs(_filters) do
			local col = t.__columns[name]
			
			if (not col) then error(name .. " not present in entity") end
			local name = col.name
			local fn = col.onEscape or passoverFunction
						
			if type(val)=='string' or type(val)=='number' then
				table.insert(lines, col.column_name .. ' = ' .. fn(val))
			elseif type(val)=='table' then
				if #val>1 then
					local cp_val = table.copy(val)
					for i, v in ipairs(cp_val) do
						cp_val[i] = fn(v)
					end
					table.insert(lines, col.column_name .. ' IN (' .. table.concat(cp_val, ', ') .. ')')
				elseif val.contains then
					table.insert(lines, filters.contains(col.column_name, fn(val.contains)))
				elseif val.like then
					table.insert(lines, col.column_name .. ' LIKE ' .. fn(filters.like(val.like)) )
				elseif val.lt then
					table.insert(lines, col.column_name .. ' < ' .. fn(val.lt))
				elseif val.gt then
					table.insert(lines, col.column_name .. ' > ' .. fn(val.gt))
				elseif val.le then
					table.insert(lines, col.column_name .. ' <= ' .. fn(val.le))
				elseif val.ge then
					table.insert(lines, col.column_name .. ' >= ' .. fn(val.ge))
				elseif val.null then
					table.insert(lines, col.column_name .. ' IS NULL')
				elseif val.notnull then
					table.insert(lines, col.column_name .. ' IS NOT NULL')
				end
			end
		end
		t.filters = {}
		t.filters_concat = cosmo.make_concat( lines )
	end
end

-- ######################################### --
--  DATABASE ENGINE
-- ######################################### --


database_engine = {}

--- Initializes the database engine and its closure-controled state
function database_engine.init(engine, connection_params)
	local luasql, luasql_connect
	local db = database_engine
	local database_type = engine.options.database_type or database_type 
	local connection
	local cursors = {}
	local insertions = {}
	
	db.open_connection = db.open_connection or function()
		local luasql, err = luasql or require("luasql." .. database_type)
		local luasql_connect, err = luasql_connect or luasql[database_type]()
		connection, err = luasql_connect:connect(unpack(connection_params))
		
		return connection, err
	end  

	db.close_connection = db.close_connection or function()
		local conn = connection
		connection = nil
		
		for idx,c in ipairs(cursors) do
			if c then
				c:close();
				cursors[idx]=nil
			end
		end
		conn:close()
	end  
	
	db.get_last_id = db.get_last_id or function(connection, ...)
		return assert(connection:execute(string.format(sql.LASTID, ...)))
	end

	db.last_id = db.last_id or function(...)
		local connection = connection or assert(db.open_connection())
		
		if not connection then
			error('Connection to the database could not be established')
		end
		
		return db.get_last_id(connection, ...) 
	end

	db.exec = db.exec or function(sql, ...)
		--TODO: think about connection closing strategies
		
		local params = {...}
		
		local connection = connection or assert(db.open_connection(), "Não foi possível estabelecer conexão com a base de dados.")
		
		if not connection then
			error('Connection to the database could not be established')
		end
		
		local cursor = assert(connection:execute(string.format(sql, ...)))
		
		if cursor and type(cursor)~='number' then
			local n = #cursors+1
			cursors[n]=cursor
			
			local value = cursor:fetch({},'a')
			if not value then
				cursor:close()
				cursors[n] = nil
			end

			-- returns an iterator function
			return function()
				local valueToReturn = value
				value = value and cursor:fetch({},'a')
				if not value then
					cursor:close()
					cursors[n] = nil
				end
				
				return valueToReturn
			end
		else
			return cursor
		end
	end
	
	return db
end

-- ######################################### --
--  PUBLIC API
-- ######################################### --

--- sets up specific configurations for this provider.
-- this function is executed when the engine is 
-- created. It can be used primarily to create 
-- the 'connection_string' or the 'connection_table' options from 
-- a more human-readable set of options    
-- @param engine the active Loft engine
-- @return alternative loft engine to be used or nil if the original engine is to be used 
function setup(engine)
	engine.options.connection_table = {
		engine.options.database,
		engine.options.username,
		engine.options.password,
		engine.options.hostname,
		engine.options.port,
	}

	engine.db = database_engine.init(engine, engine.options.connection_table)	

	return engine
end

--- stores an instace of an entity onto the database
-- if the entity has an id, generates an update statement
-- otherwise, generates an insert statement
-- @param engine the active Loft engine
function persist(engine, entity, id, obj)
	local t = table_fill_cosmo(engine, entity)
	obj.id = id or obj.id
	local data = {}
	local t_required = {}
	local isUpdate = false
	
	events.notify('before', 'persist', {engine=engine, entity=entity, id=id, obj=obj})
	
	-- Checking if every required field is present
	for i, column in ipairs(t.__columns) do
		
		if ( column.required ) then 
			if ( not obj[ column.alias ] and column.alias ~= 'id') then
				table.insert( t_required, column.alias )
			end
		end
		
		if ( obj[ column.alias ] ) then
			local fn = column.onEscape or passoverFunction
			table.insert(data, {				
				column_name = column.name,
				value = fn( obj[ column.alias ] )
			})
		end
		
	end
	
	if ( #t_required > 0 ) then		
		error("The following fields are absent (" .. table.concat(t_required, ',') .. ")")
	end
	
	t.data = cosmo.make_concat( data )
	t.id = obj.id
	
	local query
	if ( t.id ) then
		filters_fill_cosmo(t, { id = id })	
		query = cosmo.fill(sql.UPDATE, t)
		isUpdate = true
	else
		query = cosmo.fill(sql.INSERT, t)
	end
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, data = pcall(engine.db.exec, query)
	
	if isUpdate then
		return ok, data
	end
	
	if ok then
		if data and type(data) == "table" then
			--TODO: refresh object with other eventual database-generated values 
			obj.id = data.id or obj.id 
		elseif not isUpdate and not obj.id then
			obj.id = engine.db.last_id()
		end
		
		events.notify('after', 'persist', {engine=engine, entity=entity, id=id, obj=obj, data=data })
		
		return true, obj.id
	else
		events.notify('error', 'persist', {engine=engine, entity=entity, id=id, obj=obj, message=data})
		
		return nil, data
	end 
end

function create(engine, entity)
	local t = table_fill_cosmo(engine, entity)
	local query = cosmo.fill(sql.CREATE, t)
	--TODO: proper error handling
	--TODO: think about query logging strategies
	
	return pcall(engine.db.exec, query)
end

--- Eliminates a record  from the persistence that corresponds to the given id 
-- @param engine the active Loft engine
-- @param entity the schema entity identifying the type of the object to remove
-- @param id identifier of the object to remove
-- @param obj the object itself
function delete(engine, entity, id, obj)	
	local t = table_fill_cosmo(engine, entity)	
	filters_fill_cosmo(t, { id = id })	
	
	local query = cosmo.fill(sql.DELETE, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	return pcall(engine.db.exec, query)
end

-- retrieve(engine, entity, id)
--- Obtains a table from the persistence that 
-- has the proper structure of an object of a given type
-- @param engine the active Loft engine
-- @param entity the schema entity identifying the type of the object to retrieve
-- @param id identifier of the object to load
-- @return object of the given type corresponding to Id or nil
function retrieve(engine, entity, id)
	local t = table_fill_cosmo(engine, entity)	
	filters_fill_cosmo(t, { id = id })	

	local query = cosmo.fill(sql.SELECT, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, iter = pcall(engine.db.exec, query)
	
	if iter then
		return iter()
	else
		return nil, iter
	end 
end

-- search(engine, options)
--- Perform a visitor function on every record obtained in the persistence through a given set of filters
-- @param engine the active Loft engine
-- @param options the
-- 			entity the schema entity identifying the type of the object to retrieve
-- 			filters table containing a set of filter conditions
-- 			pagination table containing a pagination parameters
-- 			sorting table containing a sorting parameters
-- 			visitor	(optional) function to be executed 
-- 					every time an item is found in persistence
--					if ommited, function will return a list with everything it found
-- @return 			array with every return value of the resultset, after treatment by the visitor 
function search(engine, options)
	local entity, _filters, pagination, sorting, visitorFunction =
 		(options.entity or options[1]), options.filters, options.pagination, options.sorting, options.visitor
	
	local t = table_fill_cosmo(engine, entity)
	
	if ( type(pagination) == "table" and table.count(pagination) > 0) then
		local limit = pagination.limit or pagination.top or options.page_size or engine.options.page_size
		local offset = pagination.offset or (pagination.page and limit * (pagination.page - 1))
		
		t.pagination = {limit=limit, offset=offset}
	end
	
	if ( type(sorting) == "table" and #sorting > 0 ) then
		local sortingcolumns = {}
		for i, v in ipairs(sorting) do
			local d = string.sub(string.gsub(v,'[^+%-]*', ''), 1, 1)
			local f = string.gsub(v,'[+%-]*', '')
			if d=='-' then
				table.insert( sortingcolumns, escape_field_name(f)..' DESC')
			else
				table.insert( sortingcolumns, escape_field_name(f)..' ASC')
			end
		end
		
		t.sorting = {}
		t.sorting_concat = cosmo.make_concat( sortingcolumns )
		
	end
	
	filters_fill_cosmo(t, _filters)

	local query = cosmo.fill(sql.SELECT, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, iter = pcall(engine.db.exec, query)
	
	if ok then
		--TODO: implement resultset proxies using the list module
		local results = {}
		local fn = visitorFunction or passover_function
		local row = iter() 
		while row do
			local o = fn(row)
			table.insert(results, o)
			row = iter()
		end
		return results
	else
		return nil, iter
	end 
end

function get_tables(engine, options)
	local query = cosmo.fill(sql.GET_TABLES, {})
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, iter = pcall(engine.db.exec, query)
	
	if ok then
		--TODO: implement resultset proxies using the list module
		local results = {}
		local row = iter() 
		while row do
			local i, value = next(row)
			table.insert(results, value)
			row = iter()
		end
		return results
	else
		return nil, iter
	end
end

local function extract_type_in_description(type)
	if (string.find(type, "%(")) then --discovery size in type
		return string.match(type, "^([^(]-)%(([^)]-)%)")
	else
		return type
	end
end

function convert_description_in_table(row)
	local _type, size = extract_type_in_description(row.Type)
	local t = {}
	t.field = row.Field
	t.primary = (row.Key == "PRI") and true or nil
	t.required = (row.Null == "YES") and true or nil
	t.type = string.upper(_type)
	t.size = size or nil
	t.autoincrement = (row.Extra == "auto_increment") and true or nil
	return t
end

function get_description(engine, options)
	local table_name = options.table_name
	assert(table_name, "necessário informar o nome da tabela")
	
	local query = cosmo.fill(sql.GET_DESCRIPTION, {
		table_name = table_name
	})
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, iter = pcall(engine.db.exec, query)
	
	if ok then
		--TODO: implement resultset proxies using the list module
		local results = {}
		local row = iter() 
		while row do
			table.insert(results, convert_description_in_table(row))
			row = iter()
		end
		return results
	else
		return nil, iter
	end
end
-- count(engine, options)
--- Gets the number of results of a given set of search options 
-- @param engine the active Loft engine
-- @param options the
-- 			entity the schema entity identifying the type of the object to retrieve
-- 			filters table containing a set of filter conditions
-- @return 			number of results to be expected with these options

function count(engine, options)
 	local entity, _filters, pagination, sorting, visitorFunction =
 		options.entity, options.filters, options.pagination, options.sorting, options.visitor
 		
	local t = table_fill_cosmo(engine, entity)
	
	filters_fill_cosmo(t, _filters)
	t.columns = cosmo.make_concat( { { func = 'COUNT(*)', alias = 'count' }} )
	local query = cosmo.fill(sql.SELECT, t)
	
	local ok, iter_num = pcall(engine.db.exec, query)
	
	if ok then
		--TODO: implement resultset proxies using the list module
		local results = {}
		local row = iter_num() 
		if row then
			return row.count
		end
	end
	
	return nil
end
