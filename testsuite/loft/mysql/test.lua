require'luarocks.require'

package.path = [[;;./net.luaforge.loft/source/lua/5.1/?.lua;./net.luaforge.loft/source/lua/5.1/?/init.lua;]]

require'loft'

local options = {	
	HOSTNAME 	= "defender";
	USERNAME 	= "root";
	PASSWORD 	= "senha";
	PORT 		= "3304";
	SOURCENAME  = "goldencross_callcenter"
}

local loft = loft.initialize('database.mysql', options)

loft.registerSchema('t_loginusers', { 
	F_City='';
	F_Email='';
	F_UserID=0;
	F_Password='';
	F_Date='';
	F_Name='';
	F_State='';
	F_Gender=''; 
})

local grupo = loft.new("t_loginusers")

table.foreach(grupo, print)

local t = loft.findAll({}, 't_loginusers')

table.foreach(t, function(i, o) print('>', i) table.foreach(o, print) end)