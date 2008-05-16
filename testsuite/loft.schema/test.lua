package.path = [[;;./net.luaforge.loft/source/lua/5.1/?.lua;./net.luaforge.loft/source/lua/5.1/?/init.lua;]]


require "loft.schema"

local allTypes = Schema 'Cli' {

	 Type 'tipoDocumento'
	 	. nome "Nome"
	 	. descricao;

	 Type 'Cliente' {tableName='tbCliente', databaseName='teste'}
		. nome
		. endereco
		. telefone
		. cep : Number(256)
		. idade
		. dataNascimento : Date('now')
		. tipoDocumento : Reference('tipoDocumento');
		
	 Type 'Assinatura'
	 	. tipo {
	 		Type 'TipoAssinatura'
	 			. nome
	 			. descricao
	 	}
	}

for typeName, t in pairs(allTypes) do
	print'---------------------------'
	print(t['.typeName'])
	print'---------------------------'
	table.foreach(t['.fields'], function(_,f)
		print('   -- ' .. _, type(f))
		table.foreach(f, function(k,v)
			print('    . ' .. k, v)		
		end)

	end)
	print'---------------------------'
end
