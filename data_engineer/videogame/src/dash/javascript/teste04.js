JsonEntrada = '{"numeroCPFCNPJ": "11923474634","linhaCredito": "CPR_CUSTEIOS","propriedadesRurais": [{"numeroCAR": "GO5219803C891C6187E7145CD8AFA6EF1A0236642","numeroMatricula": "1689","caracteristicaPropriedade": "IMOVEL_MEU_E_OUTROS","formaAtuacao": "OUTROS","areaTotalImovel": 29473367.0,"recebimentoArrendamento": 50000.0,"pagamentoArrendamento": 0.0,"dataFinalContrato": "2024-08-31T02:40:26.827+00:00","receitaAgropecuaria": {"producaoAgricola": [{"produtividade": "AGRICOLA","produto": "SOJA","variedade": "SOJA PEQUENA","safra": "SAFRA 1","especificacao": "SACO 60KG","area": 80.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.3,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}},{"produtividade": "AGRICOLA","produto": "CANA-DE-ACUCAR","variedade": "CANA","safra": "SAFRA 1","especificacao": "SACO 60KG","area": 95.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.0,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}}],"pecuariaCorte": [{"produtividade": "PECUARIA_CORTE","modeloProducao": "Cria","areaPastagem": 110.0,"totalAnimais": 4000,"quantidadeVendaBezerro": 40,"precoMedioCabeca": 100.0,"terminacao":{"tipo": "Confinamento","quantidades": {"macho-14-arrobas": 5.0,"femea-14-arrobas": 0.0,"macho-18-arrobas": 10.0,"femea-16-arrobas": 8.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 15.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}},{"produtividade": "PECUARIA_CORTE","modeloProducao": "ENGORDA","areaPastagem": 324.0,"totalAnimais": 1005,"quantidadeVendaBezerro": 0,"precoMedioCabeca": 150.0,"terminacao":{"tipo": "CONFINAMENTO","quantidades":{"macho-14-arrobas": 10.0,"femea-14-arrobas": 10.0,"macho-18-arrobas": 15.0,"femea-16-arrobas": 15.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 10.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}}],"pecuariaLeite": [{"produtividade": "PECUARIA_LEITE","totalVacas": 90,"produtividadeMedia": 30.0,"area": 10.0,"pracaComercializacao": "São Domingos, GO"}]}}]}';


// Criação e tratamento de JSON E-AGRO
function eagro(listAgro) {
  var listpropriedadesRurais = ' ';

  try {
    if (listAgro != '' && listAgro != '##') {
      var jsonArrendament = JSON.parse(listAgro);

      for (var propriedadesRurais in jsonArrendament['propriedadesRurais']) {
        var atividade = '';
        var precoComercializacao = '';
        var produtividade = '';
        var area = '';
        var recebimentoArrendamento = '';
        var pagamentoArrendamento = '';
        var modeloProducao = '';
        var precoMedioCabeca = '';
        var macho14arrobas = '';
        var femea14arrobas = '';
        var macho18arrobas = '';
        var femea16arrobas = '';
        var macho22arrobas = '';
        var femea20arrobas = '';
        var precoMedioPorArroba = '';
        var totalVacas = '';
        var produtividadeMedia = '';
        var precoComercializacaoLeite = '';

        var producaoAgricola = jsonArrendament['propriedadesRurais'][propriedadesRurais]['receitaAgropecuaria']['producaoAgricola'];
        for (var i = 0; i < producaoAgricola.length; i++) {
          if (producaoAgricola[i]['atividade']) {
            atividade = producaoAgricola[i]['atividade'];
          }
          if (producaoAgricola[i]['precoComercializacao']) {
            precoComercializacao = producaoAgricola[i]['precoComercializacao'];
          }
          if (producaoAgricola[i]['produtividade']) {
            produtividade = producaoAgricola[i]['produtividade'];
          }
          if (producaoAgricola[i]['area']) {
            area = producaoAgricola[i]['area'];
          }
        }

        var propriedades = jsonArrendament['propriedadesRurais'][propriedadesRurais];
        if (propriedades['recebimentoArrendamento']) {
          recebimentoArrendamento = propriedades['recebimentoArrendamento'];
        }
        if (propriedades['pagamentoArrendamento']) {
          pagamentoArrendamento = propriedades['pagamentoArrendamento'];
        }

        var pecuariaCorte = propriedades['receitaAgropecuaria']['pecuariaCorte'];
        for (var j = 0; j < pecuariaCorte.length; j++) {
          if (pecuariaCorte[j]['modeloProducao']) {
            modeloProducao = pecuariaCorte[j]['modeloProducao'];
          }
          if (pecuariaCorte[j]['precoMedioCabeca']) {
            precoMedioCabeca = pecuariaCorte[j]['precoMedioCabeca'];
          }
          if (pecuariaCorte[j]['terminacao'] && pecuariaCorte[j]['terminacao']['quantidades']) {
            var quantidades = pecuariaCorte[j]['terminacao']['quantidades'];
            if (quantidades['macho-14-arrobas']) {
              macho14arrobas = quantidades['macho-14-arrobas'];
            }
            if (quantidades['femea-14-arrobas']) {
              femea14arrobas = quantidades['femea-14-arrobas'];
            }
            if (quantidades['macho-18-arrobas']) {
              macho18arrobas = quantidades['macho-18-arrobas'];
            }
            if (quantidades['femea-16-arrobas']) {
              femea16arrobas = quantidades['femea-16-arrobas'];
            }
            if (quantidades['macho-22-arrobas']) {
              macho22arrobas = quantidades['macho-22-arrobas'];
            }
            if (quantidades['femea-20-arrobas']) {
              femea20arrobas = quantidades['femea-20-arrobas'];
            }
          }
          if (pecuariaCorte[j]['terminacao'] && pecuariaCorte[j]['terminacao']['precoMedioPorArroba']) {
            precoMedioPorArroba = pecuariaCorte[j]['terminacao']['precoMedioPorArroba'];
          }
        }

        var pecuariaLeite = propriedades['receitaAgropecuaria']['pecuariaLeite'];
        for (var k = 0; k < pecuariaLeite.length; k++) {
          if (pecuariaLeite[k]['totalVacas']) {
            totalVacas = pecuariaLeite[k]['totalVacas'];
          }
          if (pecuariaLeite[k]['produtividadeMedia']) {
            produtividadeMedia = pecuariaLeite[k]['produtividadeMedia'];
          }
          if (pecuariaLeite[k]['pracaComercializacao']) {
            precoComercializacaoLeite = pecuariaLeite[k]['pracaComercializacao'];
          }
        }

        var valores = atividade + ";" + precoComercializacao + ";" + produtividade + ";" + area + ";" + recebimentoArrendamento + ";" + pagamentoArrendamento + ";" + modeloProducao + ";" + precoMedioCabeca + ";" + macho14arrobas + ";" + femea14arrobas + ";" + macho18arrobas + ";" + femea16arrobas + ";" + macho22arrobas + ";" + femea20arrobas + ";" + precoMedioPorArroba + ";" + totalVacas + ";" + produtividadeMedia + ";" + precoComercializacaoLeite;
        if (propriedadesRurais == 0) {
          listpropriedadesRurais = valores;
        } else {
          listpropriedadesRurais = listpropriedadesRurais + "#" + valores;
        }
      }
    }
    return listpropriedadesRurais;
  } catch (e) {
    return listpropriedadesRurais;
  }
}


console.log(eagro(JsonEntrada));
