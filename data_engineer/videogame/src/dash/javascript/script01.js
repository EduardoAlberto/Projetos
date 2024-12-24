// JSON de exemplo (substitua pelo seu próprio JSON)
JsonEntrada = '{"numeroCPFCNPJ": "11923474634","linhaCredito": "CPR_CUSTEIOS","propriedadesRurais": [{"numeroCAR": "GO5219803C891C6187E7145CD8AFA6EF1A0236642","numeroMatricula": "1689","caracteristicaPropriedade": "IMOVEL_MEU_E_OUTROS","formaAtuacao": "OUTROS","areaTotalImovel": 29473367.0,"recebimentoArrendamento": 50000.0,"pagamentoArrendamento": 0.0,"dataFinalContrato": "2024-08-31T02:40:26.827+00:00","receitaAgropecuaria": {"producaoAgricola": [{"produtividade": "AGRICOLA","produto": "SOJA","variedade": "SOJA PEQUENA","safra": "SAFRA 1","especificacao": "SACO 60KG","area": 80.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.3,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}},{"produtividade": "AGRICOLA","produto": "CANA-DE-ACUCAR","variedade": "CANA","safra": "SAFRA 1","especificacao": "SACO 60KG","area": 95.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.0,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}}],"pecuariaCorte": [{"produtividade": "PECUARIA_CORTE","modeloProducao": "Cria","areaPastagem": 110.0,"totalAnimais": 4000,"quantidadeVendaBezerro": 40,"precoMedioCabeca": 100.0,"terminacao":{"tipo": "Confinamento","quantidades": {"macho-14-arrobas": 5.0,"femea-14-arrobas": 0.0,"macho-18-arrobas": 10.0,"femea-16-arrobas": 8.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 15.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}},{"produtividade": "PECUARIA_CORTE","modeloProducao": "ENGORDA","areaPastagem": 324.0,"totalAnimais": 1005,"quantidadeVendaBezerro": 0,"precoMedioCabeca": 150.0,"terminacao":{"tipo": "CONFINAMENTO","quantidades":{"macho-14-arrobas": 10.0,"femea-14-arrobas": 10.0,"macho-18-arrobas": 15.0,"femea-16-arrobas": 15.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 10.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}}],"pecuariaLeite": [{"produtividade": "PECUARIA_LEITE","totalVacas": 90,"produtividadeMedia": 30.0,"area": 10.0,"pracaComercializacao": "São Domingos, GO"}]}}]}';


function teste(listAgro){
  var listpropriedadesRurais = ' '
  var jsonArrendament = JSON.parse(listAgro)

  for(var listpropriedadesRurais in producaoAgricola = jsonArrendament['propriedadesRurais'][0]['receitaAgropecuaria']['producaoAgricola']){
    jsonArrendament = listpropriedadesRurais
    }
  return listpropriedadesRurais

 

}

console.log(teste(JsonEntrada));



// Função para extrair as áreas de produção agrícola

function eagroProducaoAgricola(listAgro) {
  var jsonArrendament = JSON.parse(listAgro);
  var area = '';
  var produtividadeIrrigada = '';
  var listaAgro = '';

  var producaoAgricola = jsonArrendament['propriedadesRurais'][0]['receitaAgropecuaria']['producaoAgricola'];

  producaoAgricola.forEach(item => {
    if (item.hasOwnProperty('produtividadeIrrigada') && item['produtividadeIrrigada'].hasOwnProperty('area')) {
      if (area !== ' ') {
        area += ' ';
      }
      area += item['produtividadeIrrigada']['area'];
    }
    if (item.hasOwnProperty('produtividadeIrrigada') && item['produtividadeIrrigada'].hasOwnProperty('produtividade')) {
      if (produtividadeIrrigada !== ' ') {
        produtividadeIrrigada += ' ';
      }
      produtividadeIrrigada += item['produtividadeIrrigada']['produtividade'];
    }
  });

  var valores = area + ";" + produtividadeIrrigada;
  if (listaAgro === '') {
    listaAgro = valores;
  } else {
    listaAgro = listaAgro + "#" + valores;
  }

  return listaAgro;
}

console.log(eagroProducaoAgricola(JsonEntrada));

