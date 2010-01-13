package.path = package.path .. ";../source/?.lua;/home/alessandro/workspace/br.com.fabricadigital.publique.database.loony/lib/?.lua"

local schema = require'schema'

local default = schema.expand(function ()
	info = entity { 
		fields = { 
			id = { order = 1, colum_name = "F_InfoID", type = "key" },
			title = { type = "text", size = 100, maxlength=250 },
			summary=long_text(),
			fullText=long_text(),							
			section = text(),
			authorName = text(),
			authorMail = text(),
			actor = text(),
			creatorActor = text(),
			state = {
				size=10, description="Estado da info, pode assumir um de '5' valores",
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


local loft = require 'loft'

local L = loft.init({
	sourcename = "",
	username = "",
	password = "",
	hostname = "",
	port = "",	
	provider = "base",
})

local s = L.decorate(default)

local info = s.info:new()

table.foreach(info, print)



--~ local section = s.section:new()

--~ print(section:new())






