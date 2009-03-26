provider = require"loft.providers.database.sqlite3"
provider.options{
	PERSISTENCE_FILENAME = 'test.db3';
	PERSISTENCE_PATH= '/tmp/net.luaforge.loft/testsuite/'
}
provider.initialize('test.db3')

local filename = "../loft.provider.database.generic/_test.lua"
if (not loadfile(filename)) then
	filename = "testsuite/loft.provider.database.generic/_test.lua"
end

local run, err = loadfile(filename)
if (not run) then
	print(err)
else
	run()
end

provider = nil