package.path = package.path .. ";;../../source/?.lua"

local schema = require'schema'

package.preload['luasql.base']= function()
	return {
		connect = function(...)
			
		end,
	}
end

local default = schema.expand(function ()
	info = entity {
		name = 'info',
		table_name= "T_Info",
		fields = { 
			id = { order = 1, column_name = "F_InfoID", type = "key", required=true},
			title = { type = "text", size = 100, maxlength=250 },
			summary=long_text(),
			fullText=long_text(),							
			section = text(),
			authorName = text(),
			authorMail = text(),
			actor = text(),
			creatorActor = text(),
			state = {
				type = 'integer', size=10, description="Estado da info, pode assumir um de '5' valores",
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
		fields = { 
			id = { order = 1, colum_name = "F_SectionID", type = "key" },
			name = { type = "text", size = 100, maxlength=250 },
		},
		handlers = {
	        before_save = function(e, obj) print('!!!!!!!!') end,
	    },
	}	
	
end)

print( create(nil, default.entities.info) )

print( persist(nil, default.entities.info, nil, {
	summary = "Resumo",
	fullText = "Texto",
	authorName = "autor"
}) )

print( persist(nil, default.entities.info, 1, {
	summary = "Resumo",
	id = 1,
	fullText = "Texto",
	authorName = "autor"
}) )

print( retrieve(nil, default.entities.info, 1) )

print( delete(nil, default.entities.info, 1) )

print( search(nil, default.entities.info, { 
	title = 'aaaa',
	authorName = { like = "b"},
	id = 1,
	state = {1, 2, 3, 4},
	summary = { gt = "a" }
	}) 
)

print( search(nil, default.entities.info, { 
	
	}) 
)

print( search(nil, default.entities.info, nil))

print( search(nil, default.entities.info, nil, { limit = 1, offset = 1 }) )

print( search(nil, default.entities.info, nil, nil, {"title+", "-id", "+fulltext+"}) )