provider = require"loft.providers.database.sqlite3"
provider.options{
	PERSISTENCE_FILENAME = 'test.db3';
	PERSISTENCE_PATH= '/tmp/net.luaforge.loft/testsuite/'
}
provider.initialize('test.db3')

if (loadfile("../loft.provider.database.generic/_test.lua")) then
	loadfile("../loft.provider.database.generic/_test.lua")()
else
	loadfile("testsuite/loft.provider.database.generic/_test.lua")()
end
provider = nil