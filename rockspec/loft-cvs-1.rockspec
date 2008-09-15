package = "loft"
version = "cvs-1"
source = {
   url = "cvs://:pserver:anonymous:@cvs.luaforge.net:/cvsroot/loft",
   cvs_tag = "HEAD",
}
description = {
   summary = "Loft",
   detailed = [[Loft is an object factory designed to implement create, save, retrieve, search and destroy operations on lua 'objects' -- meaning by object a table that obeys a certain 'schema'. Schemas and the actual persistence operations are treated by plugins.]],
   license = "MIT/X11",
   homepage = "http://loft.luaforge.net/"
}
dependencies = {
   "lua >= 5.1"
}
build = {
   type = "none",
   install = { 
   		lua = {
   			["loft.init"] = [[../source/lua/5.1/loft/init.lua]],
   			["loft.utils"] = [[../source/lua/5.1/loft/utils.lua]],
   			["loft.providers.database.sqlite3"] = [[../source/lua/5.1/loft/providers/database/sqlite3.lua]],
   			["loft.providers.sql.sqlite3"] = [[../source/lua/5.1/loft/providers/sql/sqlite3.lua]],
   		}
   	}
}