require 'util'

module(..., package.seeall)

local api = {}

local criteria_mt = {
	__index = api
}

--- Creates a criteria table from an entity
-- this function is executed when a search is preparing
-- the SQL will be generated from the resulting table 
function create(engine, entity, include_fields, exclude_fields)
	local criteria =setmetatable({}, criteria_mt)
	criteria.__engine = engine;
	criteria.__entity = entity;
	criteria.__columns = {};
	criteria.__fields = {};
	
	criteria.__include_fields = util.indexed_table(include_fields or {})
	criteria.__exclude_fields = util.indexed_table(exclude_fields or {})
	
	criteria.table_name = entity['table_name'] or entity['name']
	
	for field_name, field in pairs(entity.fields) do
		if not field.virtual and field.column_name then
			if not criteria.__exclude_fields[field_name] and ((not include_fields) or criteria.__include_fields[field_name]) then
				criteria:field(field_name)
			else
				criteria:column(field_name)
			end
		end
	end

	criteria:sort_fields(function(f1,f2) return f1.name < f2.name end)
	criteria:sort_fields(function(f1,f2) return f1.order < f2.order end)
	
	return criteria
end

local function standard_getter_setter(criteria, name, value)
	if not value then
		return criteria['__'..name];
	end
	
	criteria['__'..name] = value
	
	return criteria
end

function api.entity(criteria, entity)
	return standard_getter_setter(criteria, 'entity', entity)
end

function api.engine(criteria, engine)
	return standard_getter_setter(criteria, 'engine', engine)
end

function api.schema(criteria, schema)
	assert(not schema, "cannot set schema on a query")
	local engine = criteria:engine() or {}
	return engine.schema;
end

function api.entities(criteria, entities)
	assert(not entities, "cannot set entities on a query")
	local schema = criteria:schema() or {}
	return schema.entities;
end

function api.provider(criteria, provider)
	assert(not provider, "cannot set provider on a query")
	return criteria:engine().provider;
end

local function create_column(criteria, field_name, field_entity)
---print(']]]]create_column]]]',field_name, field_entity and field_entity.name)
	local provider = criteria:provider();
	local entity = criteria:entity()
---print('&&&&',field_name)
	if criteria.__columns[field_name] then
---print('&_&_&_&', criteria.__columns[field_name].entity_name)
		return criteria.__columns[field_name]
	end
	
	-- find entity and column
	local relations, attribute_name = util.split_field_name(field_name)
--	print(#relations, attribute_name, field_name)
	local parent_relation

	if not field_entity then
		for _, relation_name in ipairs(relations) do
---print('[*]',relation_name, "'under'", parent_relation)
			_, entity = criteria:join(relation_name, parent_relation)
			parent_relation = relation_name
		end
	else
		entity = field_entity
		parent_relation = parent_relation or relations[#relations] or entity.name
	end

	local field = assert(entity.fields[attribute_name], "Could not find field '" .. attribute_name .. "' in entity '" .. entity.name .. "'")

	local field_type = provider.field_types[field.type] or {}
	local column = criteria.__columns[field_name] or {} -- uses the stub when a join has already created it
	column = table.add(column, field_type)
	column = table.add(column, field)
---print('[[', field.column_name, ']]')
	column.name = field.column_name
	column.alias = field_name
	column.order = field.order or 999
	column.type = field_type.type

	column.entity = entity
	column.entity_name = parent_relation or entity.name
---print('[<[', column.entity_name, ']<]')
	
	criteria.__columns[field_name] = column
	table.insert(criteria.__columns, column)
	
	return column
end

function api.create_condition(criteria, field_name, field_condition, field_entity, condition_entity)
---print('>>create_condition>>>',field_name,field_entity and field_entity.name, condition_entity and condition_entity.name)
	
	local left_side = create_column(criteria, field_name, field_entity)
	local right_side
	
	if type(field_condition)=='string' or type(field_condition)=='number' or type(field_condition)=='boolean' then
		right_side = { type = 'simple', value = field_condition }
	elseif type(field_condition)=='table' and #field_condition>1 then
		right_side = { type = 'set', value = field_condition }
	else
		---print('-->')
		right_side = field_condition
		if type(field_condition)=='table' then
			local op,c = next(field_condition) 
			if type(c)=='table' and c.field then
				local column = create_column(criteria, c.field, condition_entity)
				right_side[op] = table.merge(right_side, column)
			end
		end
	end
---print('<<create_condition<<<')
	return {left_side=left_side, right_side=right_side}
end

function api.conditions(criteria, filters)
	if not filters then
		return criteria.__conditions or {}
	end

	criteria.__conditions = criteria.__conditions or {}
	
	if ( type(filters) == "table" and next(filters)) then
		for lside, rside in pairs(filters) do
			table.insert(criteria.__conditions, criteria:create_condition(lside, rside))
		end
		--TODO: sort condition columns by order and name
	end
	
	return criteria, criteria.__conditions
end

function api.render_conditions(query, conditions, as_string)
	local provider = query:provider()
	local T = query:template()

	local result = {};

	for _, condition in ipairs(conditions) do
		local left_side = condition.left_side
		local right_side = condition.right_side
		
		local escape_function = left_side.onEscape or passover_function
		local fn = function(v)
			if type(v)=='table' then 
				return T.field_name(query, v)
			else
				return escape_function(v)
			end 
		end
		
		local lside = T.field_name(query, left_side)
		local op, rside
					
		if right_side.type == 'simple' then
			op, rside = T.EQ, fn(right_side.value)
		elseif type(right_side)=='table' then
			if right_side.type=='set' or right_side.notin then
				local copied_items = table.copy(right_side.value or right_side.notin)
				for i, v in ipairs(copied_items) do
					copied_items[i] = fn(v)
				end
				op, rside = right_side.notin and T.NOTIN or T.IN, T.set(copied_items)
			elseif right_side.contains then
				lside, op, rside = '', '', provider.filters.contains(lside, fn(right_side.contains))
			elseif right_side.like then
				op, rside = T.LIKE, fn(provider.filters.like(right_side.like))
			elseif right_side.eq then
				op, rside = T.EQ, fn(right_side.eq)
			elseif right_side.lt then
				op, rside = T.LT, fn(right_side.lt)
			elseif right_side.gt then
				op, rside = T.GT, fn(right_side.gt)
			elseif right_side.le then
				op, rside = T.LE, fn(right_side.le)
			elseif right_side.ge then
				op, rside = T.GE, fn(right_side.ge)
			elseif right_side.null then
				op, rside = T.ISNULL, ''
			elseif right_side.notnull then
				op, rside = T.ISNOTNULL, ''
			end
		end
		table.insert(result, T.condition(query, lside, op, rside))
	end
	
	return as_string and T.join_conditions(result) or result
end

function api.template(criteria, template)
	if not template then
		return criteria.__template or (criteria.__renderer and criteria.__renderer.templates)
	end
	criteria.__template = template
	return criteria
end

function api.renderer(criteria, renderer)
	return  standard_getter_setter(criteria, 'renderer', renderer)
end

function api.render(criteria, options)
	local renderer = criteria:renderer() or criteria:provider().render_engine
	local filters = options.filters
	
	renderer.prepare(criteria, filters)
	
	return renderer.render(criteria, options or {})
end

function api.join(criteria, attr, left_alias)
---print('>>>join>>', attr, left_alias)
	local entity = criteria:entity()
	local entities = criteria:entities();
	
	criteria.from_alias = entity.name
	criteria.__joins = criteria.__joins or {};
---print('left_alias:', left_alias, attr)
	local left_alias = left_alias or entity.name
	local right_attribute = assert(attr, 'join must have an attribute name')

	if criteria.__joins[right_attribute] then
		-- attribute is already on a previous join
		return criteria, criteria.__joins[right_attribute]
	end
---print('==joined=>',right_attribute)

	if left_alias~=entity.name and not criteria.__joins[left_alias] then
		error("entity '"..tostring(left_alias).."' must be added to criteria before being used as the left side of a join",2)
	end
	local left = assert(entities[left_alias], "join entity '" .. tostring(left_alias).. "' must be present in schema") 

	local right_field = assert(left.fields[right_attribute], "field '"..tostring(right_attribute).."' doesn't exist on entity '"..tostring(left_alias).."'")
	local right_entity = assert(right_field.entity, "field '"..tostring(right_attribute).."' must be a relationship attribute on entity '"..tostring(left_alias).."'")

	local right = assert(entities[right_entity], "join entity '" .. tostring(right_entity).. "' must be present in schema")

	criteria.__joins[right_attribute] = right
	
	--TODO: add support for left and right joins
	--TODO: add support for key names different than 'id'
	--TODO: add support for multiple keys
	local left_condition_field = (left_alias~=entity.name and left_alias..'_' or '')..right_attribute
---print('>>',left_condition_field, left_condition_field..'_id')
	local on_conditions = { criteria:create_condition(left_condition_field, {eq = {field=left_condition_field..'_id'}}, left, right) }
	
	table.insert(criteria.__joins, {type="INNER", join_table=right.table_name, alias=right_attribute, on_clause=function()
		return criteria:render_conditions(on_conditions, true)
	end}) 

	return criteria, right
end

function api.columns(criteria, fields)
	for field_name,field in pairs(fields) do
		criteria:column(field_name,field)
	end
	return criteria, criteria.__columns
end

function api.column(criteria, field_name)
	return criteria, create_column(criteria, field_name)
end

function api.field(criteria, field_name)

	if criteria.__fields[field_name] then
		return criteria, criteria.__fields[field_name] 
	end

	local _, column = criteria:column(field_name)
	
	criteria.__fields[field_name] = column
	table.insert(criteria.__fields, column)
	
	return criteria, column
end

function api.sort_fields(criteria, fn)
	table.sort(criteria.__columns, fn)
	table.sort(criteria.__fields, fn)
	return criteria
end

function api.include_fields(criteria, fields)
	criteria.__include_fields(fields)
	return criteria			
end

function api.exclude_fields(criteria, fields)
	criteria.__exclude_fields(fields)
	return criteria			
end
 