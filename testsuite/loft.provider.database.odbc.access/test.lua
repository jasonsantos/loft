provider = require"loft.providers.database.access"
provider.initialize('loft_access')

if (loadfile("../loft.provider.database.generic/_test.lua")) then
	loadfile("../loft.provider.database.generic/_test.lua")()
else
	loadfile("testsuite/loft.provider.database.generic/_test.lua")()
end
provider = nil