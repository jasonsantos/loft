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




local newsTypes = Schema 'Newsletter' {
	Type 'Grupo'
		.nm_grupo "Nome";
	
	Type 'Veiculo' 
		.nome
		.periodicidade : Number()
		.descricao
		.subject {
			beforeSave = function(...)
				warning()
			end;
		}
		.from
		.groups : Collection(Type'Grupo')
		
}


t = newsTypes['Veiculo'].fields().names()
t = newsTypes['Veiculo'].fields().types()

loft.initialize('sqlite3')

loft.registerSchema(newsTypes)

v = new'Veiculo'

if v:validate() then

loft.persist(v)

else

local msgs = {
	unpack(v:getErrors()),
	unpack(v:getWarnings())
}

end



vf = Face 'ListaDeVeiculos' For 'Veiculo' {
	.nome 
	.teste {fieldName='descricao', size=70}
	.valor {type='currency', currency='BRL'}
	.dataNascimento {type='date', format='DD/MM/AA'}
	.grupo {
		type='collection',
		items = {
			nome = {type='text'}
		}
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
