JsonEntrada = '{"numeroCPFCNPJ": "11923474634","linhaCredito": "CPR_CUSTEIOS","propriedadesRurais": [{"numeroCAR": "GO5219803C891C6187E7145CD8AFA6EF1A0236642","numeroMatricula": "1689","caracteristicaPropriedade": "IMOVEL_MEU_E_OUTROS","formaAtuacao": "OUTROS","areaTotalImovel": 29473367.0,"recebimentoArrendamento": 50000.0,"pagamentoArrendamento": 0.0,"dataFinalContrato": "2024-08-31T02:40:26.827+00:00","receitaAgropecuaria": {"producaoAgricola": [{"atividade": "AGRICOLA","produto": "SOJA","variedade": "SOJA PEQUENA","safra": "SAFRA 1","especificacao": "SACO 60KG","precoComercializacao": 80.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.3,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}},{"atividade": "PECUARIA_CORTE","produto": "CANA-DE-ACUCAR","variedade": "CANA","safra": "SAFRA 1","especificacao": "SACO 60KG","precoComercializacao": 95.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.0,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}}],"pecuariaCorte": [{"atividade": "PECUARIA_CORTE","modeloProducao": "Cria","areaPastagem": 110.0,"totalAnimais": 4000,"quantidadeVendaBezerro": 40,"precoMedioCabeca": 100.0,"terminacao":{"tipo": "Confinamento","quantidades": {"macho-14-arrobas": 5.0,"femea-14-arrobas": 0.0,"macho-18-arrobas": 10.0,"femea-16-arrobas": 8.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 15.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}},{"atividade": "PECUARIA_CORTE","modeloProducao": "ENGORDA","areaPastagem": 324.0,"totalAnimais": 1005,"quantidadeVendaBezerro": 0,"precoMedioCabeca": 150.0,"terminacao":{"tipo": "CONFINAMENTO","quantidades":{"macho-14-arrobas": 10.0,"femea-14-arrobas": 10.0,"macho-18-arrobas": 15.0,"femea-16-arrobas": 15.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 10.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}}],"pecuariaLeite": [{"atividade": "PECUARIA_LEITE","totalVacas": 90,"produtividadeMedia": 30.0,"precoComercializacao": 10.0,"pracaComercializacao": "São Domingos, GO"}]}}]}';

// Criação e tratamento de JSON E-AGRO
function eagro(listAgro) {
  var listpropriedadesRurais = '';

  try {
    if (listAgro != '' && listAgro != '##') {
      var jsonArrendament = JSON.parse(listAgro);

      for (var propriedadesRurais in jsonArrendament['propriedadesRurais']) {
        var propriedadesInfo = '';

        var propriedade = jsonArrendament['propriedadesRurais'][propriedadesRurais];
        var recebimentoArrendamento = propriedade['recebimentoArrendamento'] || 0;  // Se vazio, coloca 0
        var pagamentoArrendamento = propriedade['pagamentoArrendamento'] || 0;  // Se vazio, coloca 0

        var producaoAgricola = propriedade['receitaAgropecuaria']['producaoAgricola'];
        for (var i = 0; i < producaoAgricola.length; i++) {
          var atividade = producaoAgricola[i]['atividade'] || '';
          var precoComercializacao = producaoAgricola[i]['precoComercializacao'] || 0;  // Se vazio, coloca 0
          
          // Separando as variáveis de produtividade irrigada
          var areaIrrigada = producaoAgricola[i]['produtividadeIrrigada'] ? producaoAgricola[i]['produtividadeIrrigada'].area || 0 : 0;  // Se vazio, coloca 0
          var produIrrigada = producaoAgricola[i]['produtividadeIrrigada'] ? producaoAgricola[i]['produtividadeIrrigada'].produtividade || 0 : 0;  // Se vazio, coloca 0
          
          // Separando as variáveis de produtividade sequeira
          var areaSequeira = producaoAgricola[i]['produtividadeSequeira'] ? producaoAgricola[i]['produtividadeSequeira'].area || 0 : 0;  // Se vazio, coloca 0
          var produSequeira = producaoAgricola[i]['produtividadeSequeira'] ? producaoAgricola[i]['produtividadeSequeira'].produtividade || 0 : 0;  // Se vazio, coloca 0

          // Concatenando todas as informações de produção agrícola
          var valores = `${atividade};${precoComercializacao};${areaIrrigada};${produIrrigada};${areaSequeira};${produSequeira};${recebimentoArrendamento};${pagamentoArrendamento}`;

          // Agora, incluímos as variáveis de pecuariaCorte
          var pecuariaCorte = propriedade['receitaAgropecuaria']['pecuariaCorte'];
          for (var j = 0; j < pecuariaCorte.length; j++) {
            var modeloProducao = pecuariaCorte[j]['modeloProducao'] || '';
            var precoMedioCabeca = pecuariaCorte[j]['precoMedioCabeca'] || 0;  // Se vazio, coloca 0
            var quantidadeVendaBezerro = pecuariaCorte[j]['quantidadeVendaBezerro'] || 0;  // Se vazio, coloca 0
            
            // Recuperando as variáveis de terminação
            var macho14Arrobas = pecuariaCorte[j]['terminacao']['quantidades']['macho-14-arrobas'] || 0;  // Se vazio, coloca 0
            var femea14Arrobas = pecuariaCorte[j]['terminacao']['quantidades']['femea-14-arrobas'] || 0;  // Se vazio, coloca 0
            var macho18Arrobas = pecuariaCorte[j]['terminacao']['quantidades']['macho-18-arrobas'] || 0;  // Se vazio, coloca 0
            var femea16Arrobas = pecuariaCorte[j]['terminacao']['quantidades']['femea-16-arrobas'] || 0;  // Se vazio, coloca 0
            var macho22Arrobas = pecuariaCorte[j]['terminacao']['quantidades']['macho-22-arrobas'] || 0;  // Se vazio, coloca 0
            var femea20Arrobas = pecuariaCorte[j]['terminacao']['quantidades']['femea-20-arrobas'] || 0;  // Se vazio, coloca 0

            var precoMedioPorArroba = pecuariaCorte[j]['terminacao']['precoMedioPorArroba'] || 0;  // Se vazio, coloca 0

            // Adicionando essas variáveis ao final de cada registro
            valores += `;${modeloProducao};${precoMedioCabeca};${quantidadeVendaBezerro};${macho14Arrobas};${femea14Arrobas};${macho18Arrobas};${femea16Arrobas};${macho22Arrobas};${femea20Arrobas};${precoMedioPorArroba}`;
          }

          // Agora, incluímos as variáveis de pecuariaLeite
          var pecuariaLeite = propriedade['receitaAgropecuaria']['pecuariaLeite'];
          for (var k = 0; k < pecuariaLeite.length; k++) {
            var totalVacas = pecuariaLeite[k]['totalVacas'] || 0;  // Se vazio, coloca 0
            var produtividadeMedia = pecuariaLeite[k]['produtividadeMedia'] || 0;  // Se vazio, coloca 0
            var precoComercializacaoLeite = pecuariaLeite[k]['precoComercializacao'] || 0;  // Se vazio, coloca 0

            // Adicionando as variáveis de leite
            valores += `;${totalVacas};${produtividadeMedia};${precoComercializacaoLeite}`;
          }

          if (propriedadesInfo !== '') {
            propriedadesInfo += '#'; // Removeu o espaço entre os #
          }
          propriedadesInfo += valores;
        }

        // Concatenando os resultados das propriedades
        if (listpropriedadesRurais.trim() === '') {
          listpropriedadesRurais = propriedadesInfo;
        } else {
          listpropriedadesRurais = listpropriedadesRurais + "#" + propriedadesInfo;
        }
      }
    }
    return listpropriedadesRurais;
  } catch (e) {
    return listpropriedadesRurais;
  }
}

console.log(eagro(JsonEntrada));
