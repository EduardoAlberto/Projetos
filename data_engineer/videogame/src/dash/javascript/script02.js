// JSON de exemplo (substitua pelo seu próprio JSON)
const JsonEntrada = '{"numeroCPFCNPJ": "11923474634","linhaCredito": "CPR_CUSTEIOS","propriedadesRurais": [{"numeroCAR": "GO5219803C891C6187E7145CD8AFA6EF1A0236642","numeroMatricula": "1689","caracteristicaPropriedade": "IMOVEL_MEU_E_OUTROS","formaAtuacao": "OUTROS","areaTotalImovel": 29473367.0,"recebimentoArrendamento": 50000.0,"pagamentoArrendamento": 0.0,"dataFinalContrato": "2024-08-31T02:40:26.827+00:00","receitaAgropecuaria": {"producaoAgricola": [{"produtividade": "AGRICOLA","produto": "SOJA","variedade": "SOJA PEQUENA","safra": "SAFRA 1","especificacao": "SACO 60KG","area": 80.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.3,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}},{"produtividade": "AGRICOLA","produto": "CANA-DE-ACUCAR","variedade": "CANA","safra": "SAFRA 1","especificacao": "SACO 60KG","area": 95.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.0,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}}],"pecuariaCorte": [{"produtividade": "PECUARIA_CORTE","modeloProducao": "Cria","areaPastagem": 110.0,"totalAnimais": 4000,"quantidadeVendaBezerro": 40,"precoMedioCabeca": 100.0,"terminacao":{"tipo": "Confinamento","quantidades": {"macho-14-arrobas": 5.0,"femea-14-arrobas": 0.0,"macho-18-arrobas": 10.0,"femea-16-arrobas": 8.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 15.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}},{"produtividade": "PECUARIA_CORTE","modeloProducao": "ENGORDA","areaPastagem": 324.0,"totalAnimais": 1005,"quantidadeVendaBezerro": 0,"precoMedioCabeca": 150.0,"terminacao":{"tipo": "CONFINAMENTO","quantidades":{"macho-14-arrobas": 10.0,"femea-14-arrobas": 10.0,"macho-18-arrobas": 15.0,"femea-16-arrobas": 15.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 10.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}}],"pecuariaLeite": [{"produtividade": "PECUARIA_LEITE","totalVacas": 90,"produtividadeMedia": 30.0,"area": 10.0,"pracaComercializacao": "São Domingos, GO"}]}}]}';

// Função para extrair as áreas, produtividade irrigada, recebimentoArrendamento, pagamentoArrendamento, atividade, precoComercializacao, modeloProducao, precoMedioCabeca, quantidadeVendaBezerro, macho-14-arrobas, fêmea-14-arrobas, macho-18-arrobas, precoMedioPorArroba, totalVacas, produtividadeMedia e precoComercializacao
function eagroProducaoAgricola(listAgro) {
  var jsonArrendament = JSON.parse(listAgro);
  var area = '';
  var produtividadeIrrigada = '';
  var listaAgro = '';
  var atividade = '';
  var precoComercializacao = '';
  var modeloProducao = '';
  var precoMedioCabeca = '';
  var quantidadeVendaBezerro = '';
  var macho14arrobas = '';
  var femea14arrobas = '';
  var macho18arrobas = '';
  var precoMedioPorArroba = '';
  var totalVacas = '';
  var produtividadeMedia = '';
  var precoComercializacaoLeite = '';

  var propriedadesRurais = jsonArrendament['propriedadesRurais'][0];
  var producaoAgricola = propriedadesRurais['receitaAgropecuaria']['producaoAgricola'];
  var pecuariaCorte = propriedadesRurais['receitaAgropecuaria']['pecuariaCorte'];
  var pecuariaLeite = propriedadesRurais['receitaAgropecuaria']['pecuariaLeite'];

  var recebimentoArrendamento = propriedadesRurais['recebimentoArrendamento'];
  var pagamentoArrendamento = propriedadesRurais['pagamentoArrendamento'];

  producaoAgricola.forEach(item => {
    if (item.hasOwnProperty('produtividadeIrrigada') && item['produtividadeIrrigada'].hasOwnProperty('area')) {
      if (area !== '') {
        area += ', ';
      }
      area += item['produtividadeIrrigada']['area'];
    }
    if (item.hasOwnProperty('produtividadeIrrigada') && item['produtividadeIrrigada'].hasOwnProperty('produtividade')) {
      if (produtividadeIrrigada !== '') {
        produtividadeIrrigada += ', ';
      }
      produtividadeIrrigada += item['produtividadeIrrigada']['produtividade'];
    }
    if (item.hasOwnProperty('atividade')) {
      if (atividade !== '') {
        atividade += ', ';
      }
      atividade += item['atividade'];
    }
    if (item.hasOwnProperty('pracaComercializacao')) {
      if (precoComercializacao !== '') {
        precoComercializacao += ', ';
      }
      precoComercializacao += item['pracaComercializacao'];
    }
  });

  pecuariaCorte.forEach(item => {
    if (item.hasOwnProperty('modeloProducao')) {
      if (modeloProducao !== '') {
        modeloProducao += ', ';
      }
      modeloProducao += item['modeloProducao'];
    }
    if (item.hasOwnProperty('precoMedioCabeca')) {
      if (precoMedioCabeca !== '') {
        precoMedioCabeca += ', ';
      }
      precoMedioCabeca += item['precoMedioCabeca'];
    }
    if (item.hasOwnProperty('quantidadeVendaBezerro')) {
      if (quantidadeVendaBezerro !== '') {
        quantidadeVendaBezerro += ', ';
      }
      quantidadeVendaBezerro += item['quantidadeVendaBezerro'];
    }
    if (item.hasOwnProperty('terminacao') && item['terminacao'].hasOwnProperty('quantidades')) {
      var quantidades = item['terminacao']['quantidades'];
      if (quantidades.hasOwnProperty('macho-14-arrobas')) {
        if (macho14arrobas !== '') {
          macho14arrobas += ', ';
        }
        macho14arrobas += quantidades['macho-14-arrobas'];
      }
      if (quantidades.hasOwnProperty('femea-14-arrobas')) {
        if (femea14arrobas !== '') {
          femea14arrobas += ', ';
        }
        femea14arrobas += quantidades['femea-14-arrobas'];
      }
      if (quantidades.hasOwnProperty('macho-18-arrobas')) {
        if (macho18arrobas !== '') {
          macho18arrobas += ', ';
        }
        macho18arrobas += quantidades['macho-18-arrobas'];
      }
    }
    if (item['terminacao'].hasOwnProperty('precoMedioPorArroba')) {
      if (precoMedioPorArroba !== '') {
        precoMedioPorArroba += ', ';
      }
      precoMedioPorArroba += item['terminacao']['precoMedioPorArroba'];
    }
  });

  pecuariaLeite.forEach(item => {
    if (item.hasOwnProperty('totalVacas')) {
      if (totalVacas !== '') {
        totalVacas += ', ';
      }
      totalVacas += item['totalVacas'];
    }
    if (item.hasOwnProperty('produtividadeMedia')) {
      if (produtividadeMedia !== '') {
        produtividadeMedia += ', ';
      }
      produtividadeMedia += item['produtividadeMedia'];
    }
    if (item.hasOwnProperty('pracaComercializacao')) {
      if (precoComercializacaoLeite !== '') {
        precoComercializacaoLeite += ', ';
      }
      precoComercializacaoLeite += item['pracaComercializacao'];
    }
  });

  var valores = area + ";" + produtividadeIrrigada + ";" + recebimentoArrendamento + ";" + pagamentoArrendamento + ";" + atividade + ";" + precoComercializacao + ";" + modeloProducao + ";" + precoMedioCabeca + ";" + quantidadeVendaBezerro + ";" + macho14arrobas + ";" + femea14arrobas + ";" + macho18arrobas + ";" + precoMedioPorArroba + ";" + totalVacas + ";" + produtividadeMedia + ";" + precoComercializacaoLeite;
  if (listaAgro === '') {
    listaAgro = valores;
  } else {
    listaAgro = listaAgro + "#" + valores;
  }

  return listaAgro;
}

console.log(eagroProducaoAgricola(JsonEntrada));


area + ";" + produtividadeIrrigada + ";" + recebimentoArrendamento + ";" + pagamentoArrendamento + ";" + atividade + ";" + precoComercializacao + ";" + modeloProducao + ";" + precoMedioCabeca + ";" + quantidadeVendaBezerro + ";" + macho14arrobas + ";" + femea14arrobas + ";" + macho18arrobas + ";" + precoMedioPorArroba + ";" + totalVacas + ";" + produtividadeMedia + ";" + precoComercializacaoLeite;

atividade + ";" + precoComercLeite + ";" + recebimentoArrend + ";" + pagamentoArrend + ";" + area;


precoComercializacao + ";" + 
modeloProducao + ";" + 
precoMedioCabeca + ";" + 
quantidadeVendaBezerro + ";" + 
macho14arrobas + ";" + 
femea14arrobas + ";" + 
macho18arrobas + ";" + 
precoMedioPorArroba + ";" + 
totalVacas + ";" + 
produtividadeMedia + ";" + 
precoComercializacaoLeite


