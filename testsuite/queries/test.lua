package.path = package.path .. ";;../../source/?.lua"

pcall(require, 'luarocks.require')

local schema = require'schema'

local queries = require'loft.queries'

local default = schema.expand(function ()
	column_prefix = "f_"

	info = entity {
		table_name= "T_Info",
		fields = {
			id = { order = 1, column_name = "f_infoid", type = "key", required=true},
			title = { order=2, type = "text", size = 100, maxlength=250 },
			summary=long_text{order=3},
			fulltext=long_text(),
			section = belongs_to{'section'},
			authorName = text(),
			authorMail = text(),
			author = belongs_to{'actor'},
			creatorActor = belongs_to{'actor'},
			state = integer{
				size=10, description="Info state, can assume one of '5' values",
				handlers = {
	                get = function (f, v) record.gettings = record.gettings + 1 return v end,
	                set = function (f, v) record.settings = record.settings + 1 return v end,
	            }
			}
		},
		handlers = {
	        before_save = function(e, obj) record.lastLog = 'Saving Entity' end,
	    },
	}

	section = entity {
		table_name= "T_Section",
		fields = {
			id = { order = 1, column_name = "F_SectionID", type = "key" },
			name = { type = "text", size = 100, maxlength=250 },
			tag = { type = "text", size = 100, maxlength=250 },
			editor = has_one{ 'editor' },
			infos = has_many{ 'info' }
		},
		handlers = {
	        before_save = function(e, obj) print('!!!!!!!!') end,
	    },
	}

	editor = entity {
		table_name= "T_Editor",
		fields = {
			id = key{},
			actor = belongs_to{ column_name = "F_ActorID", entity = "actor" },
			sections = has_many{ 'section' }
		},
		handlers = {
	        before_save = function(e, obj) print('!!!!!!!!') end,
	    },
	}

	actor = entity {
		table_name= "T_Actor",
		fields = {
			id = { order = 1, column_name = "F_ActorID", type = "key" },
			name = { type = "text", size = 100, maxlength=250 },
		},
		handlers = {
	        before_save = function(e, obj) print('!!!!!!!!') end,
	    },
	}

end)


do
    -- testing create function result
    local q = queries:create()
    assert(q)
    assert(type(q)=='table')
    assert(q.entity)
    assert(q.engine)
    assert(q.schema)
    assert(q.entities)
    assert(q.provider)
    assert(q.conditions)
    assert(q.template)
    assert(q.renderer)
    assert(q.render)
    --assert(q.field)
    --assert(q.sort_fields)
    assert(q.include_fields)
    assert(q.exclude_fields)
    assert(q.fields)
end

do
    local q = queries:create()
                :fields('id', 'name', '-fulltext')

    assert(#q.__include_fields==2)
    assert(q.__include_fields['id'])
    assert(q.__include_fields['name'])
    assert(#q.__exclude_fields==1)
    assert(q.__exclude_fields['fulltext'])

    local r = queries:create()
                :fields{'id', 'name', '-fulltext'}

    assert(#r.__include_fields==2)
    assert(r.__include_fields['id'])
    assert(r.__include_fields['name'])
    assert(#r.__exclude_fields==1)
    assert(r.__exclude_fields['fulltext'])

    local t = queries:create()
                :fields[[+id, +name, -fulltext]]

    assert(#t.__include_fields==2)
    assert(t.__include_fields['id'])
    assert(t.__include_fields['name'])
    assert(#t.__exclude_fields==1)
    assert(t.__exclude_fields['fulltext'])
end

