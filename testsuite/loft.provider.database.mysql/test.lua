provider = require"loft.providers.database.mysql"
provider.initialize('loft', 'root')

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