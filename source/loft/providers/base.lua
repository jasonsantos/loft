require'util'
require'events'

local cosmo = require 'cosmo'

----------------------------------------------
-- Persistence Provider for the Loft Module
----------------------------------------------
-- 

module(..., package.seeall)

database_engine = require 'loft.database'

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
-- FUNCTIONS
-- -------------- --

-- setup(engine)

-- create(engine, entity)
-- persist(engine, entity, id, data)
-- retrieve(engine, entity, id)
-- delete(engine, entity, id)

-- search(engine, options)
-- count(engine, options)

-- -------------- --
-- BASE PROPERTIES
-- -------------- --
-- Providers that use SQL and extend Base can make use of
-- these properties to extend behavior without rewritting
-- the API functions  

-- database_engine 

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


-- ######################################### --
--  OPTIONS
-- ######################################### --
--  this provider default options


database_type = 'base'

reserved_words = util.indexed_table{}
 
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
	
	has_one={type='BIGINT', size='8', onEscape=tonumber},
	belongs_to={type='BIGINT', size='8', onEscape=tonumber},
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
	
	LASTID = [==[SELECT LAST_INSERT_ID()]==]
	
}

-- ######################################### --
--  INTERNALS
-- ######################################### --


local passover_function = function(...) return ... end

local table_fill_cosmo = function (query)
	local t = query or {}
	
	if ( not t.table_name ) then
		error("Entity must have a  `name` or `table_name`.")
	end
	
	t.columns = cosmo.make_concat( t.__fields )
	
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

local criteria_mt = {
	__index = {
		add_columns = function(criteria, fields)
			for field_name,field in pairs(fields) do
				criteria:add_column(field_name,field)
			end
			return criteria
		end, 
		add_column = function(criteria, field_name, field)
			local field_type = field_types[field.type] or {}
			local column = table.merge(field_type, field)
			
			column.name = field.column_name
			column.alias = field_name
			column.order = field.order or 999
			column.type = field_type.type
			
			criteria.__columns[field_name] = column 
			
			table.insert(criteria.__columns, criteria.__columns[field_name])
			
			return criteria
		end,
		add_field = function(criteria, field_name, field)
			
			if field then
				criteria:add_column(field_name, field)
			end
			
			criteria.__fields[field_name] = criteria.__columns[field_name]
			table.insert(criteria.__fields, criteria.__columns[field_name])
			
			return criteria
		end,
		sort_fields = function(criteria, fn)
			table.sort(criteria.__columns, fn)
			table.sort(criteria.__fields, fn)
			return criteria
		end,
		include_fields = function(criteria, fields)
			criteria.__include_fields(fields)
			return criteria			
		end,
		exclude_fields = function(criteria, fields)
			criteria.__exclude_fields(fields)
			return criteria			
		end,
	}
}

--- Creates a criteria table from an entity
-- this function is executed when a search is preparing
-- the SQL will be generated from the resulting table 
function create_query(engine, entity, include_fields, exclude_fields)
	local criteria =setmetatable({}, criteria_mt)
	criteria.__entity = entity;
	criteria.__columns = {};
	criteria.__fields = {};
	
	criteria.__include_fields = util.indexed_table(include_fields or {})
	criteria.__exclude_fields = util.indexed_table(exclude_fields or {})
	
	criteria.table_name = entity['table_name'] or entity['name']
	
	for field_name, field in pairs(entity.fields) do
		if not field.virtual and field.column_name then
			if not criteria.__exclude_fields[field_name] and ((not include_fields) or criteria.__include_fields[field_name]) then
				criteria:add_field(field_name, field)
			else
				criteria:add_column(field_name, field)
			end
		end
	end
	
	criteria:sort_fields(function(f1,f2) return f1.name < f2.name end)
	criteria:sort_fields(function(f1,f2) return f1.order < f2.order end)
	
	return criteria
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
	local query = create_query(engine, entity)
	obj.id = id or obj.id
	local data = {}
	local t_required = {}
	local isUpdate = false
	
	events.notify('before', 'persist', {engine=engine, entity=entity, id=id, obj=obj})
	
	-- Checking if every required field is present
	for i, column in ipairs(query.__columns) do
		
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
	
	local t = table_fill_cosmo(query)
	t.data = cosmo.make_concat( data )
	t.id = obj.id
	
	local sql_str
	if ( t.id ) then
		filters_fill_cosmo(t, { id = id })	
		sql_str = cosmo.fill(sql.UPDATE, t)
		isUpdate = true
	else
		sql_str = cosmo.fill(sql.INSERT, t)
	end
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, data = pcall(engine.db.exec, sql_str)
	
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
	local query = create_query(engine, entity)
	local sql_str = cosmo.fill(sql.CREATE, table_fill_cosmo(query))
	--TODO: proper error handling
	--TODO: think about query logging strategies
	
	return pcall(engine.db.exec, sql_str)
end

--- Eliminates a record  from the persistence that corresponds to the given id 
-- @param engine the active Loft engine
-- @param entity the schema entity identifying the type of the object to remove
-- @param id identifier of the object to remove
-- @param obj the object itself
function delete(engine, entity, id, obj)	
	local query = create_query(engine, entity)
	local t = table_fill_cosmo(query)	
	filters_fill_cosmo(t, { id = id })	
	
	local sql_str = cosmo.fill(sql.DELETE, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	return pcall(engine.db.exec, sql_str)
end

-- retrieve(engine, entity, id)
--- Obtains a table from the persistence that 
-- has the proper structure of an object of a given type
-- @param engine the active Loft engine
-- @param entity the schema entity identifying the type of the object to retrieve
-- @param id identifier of the object to load
-- @return object of the given type corresponding to Id or nil
function retrieve(engine, entity, id)
	local query = create_query(engine, entity)
	local t = table_fill_cosmo(query)	
	filters_fill_cosmo(t, { id = id })	

	local sql_str = cosmo.fill(sql.SELECT, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, iter = pcall(engine.db.exec, sql_str)
	
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
	local entity, filters, pagination, sorting, visitorFunction =
 		(options.entity or options[1]), options.filters, options.pagination, options.sorting, options.visitor
	
	local criteria = create_query(engine, entity, options.include_fields, options.exclude_fields)
	
	if ( type(pagination) == "table" and table.count(pagination) > 0) then
		local limit = pagination.limit or pagination.top or options.page_size or engine.options.page_size
		local offset = pagination.offset or (pagination.page and limit * (pagination.page - 1))
		
		criteria.pagination = {limit=limit, offset=offset}
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
		
		criteria.sorting = {}
		criteria.sorting_concat = cosmo.make_concat( sortingcolumns )
		
	end
	
	filters_fill_cosmo(criteria, filters)
	local t = table_fill_cosmo(criteria)	

	local sql_str = cosmo.fill(sql.SELECT, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, iter = pcall(engine.db.exec, sql_str)
	
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
	local sql_str = cosmo.fill(sql.SELECT, t)
	
	local ok, iter_num = pcall(engine.db.exec, sql_str)
	
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
