provider = require"loft.providers.database.mssql"
provider.initialize('loft_mssql')

if (loadfile("../loft.provider.database.generic/_test.lua")) then
	loadfile("../loft.provider.database.generic/_test.lua")()
else
	loadfile("testsuite/loft.provider.database.generic/_test.lua")()
end
provider = nil