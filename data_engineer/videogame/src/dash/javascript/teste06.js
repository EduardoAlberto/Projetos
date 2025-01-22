JsonEntrada = '{"numeroCPFCNPJ": "11923474634","linhaCredito": "CPR_CUSTEIOS","propriedadesRurais": [{"numeroCAR": "GO5219803C891C6187E7145CD8AFA6EF1A0236642","numeroMatricula": "1689","caracteristicaPropriedade": "IMOVEL_MEU_E_OUTROS","formaAtuacao": "OUTROS","areaTotalImovel": 29473367.0,"recebimentoArrendamento": 50000.0,"pagamentoArrendamento": 0.0,"dataFinalContrato": "2024-08-31T02:40:26.827+00:00","receitaAgropecuaria": {"producaoAgricola": [{"atividade": "AGRICOLA","produto": "SOJA","variedade": "SOJA PEQUENA","safra": "SAFRA 1","especificacao": "SACO 60KG","precoComercializacao": 80.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.3,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}},{"atividade": "PECUARIA_CORTE","produto": "CANA-DE-ACUCAR","variedade": "CANA","safra": "SAFRA 1","especificacao": "SACO 60KG","precoComercializacao": 95.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.0,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}}],"pecuariaCorte": [{"atividade": "PECUARIA_CORTE","modeloProducao": "Cria","areaPastagem": 110.0,"totalAnimais": 4000,"quantidadeVendaBezerro": 40,"precoMedioCabeca": 100.0,"terminacao":{"tipo": "Confinamento","quantidades": {"macho-14-arrobas": 5.0,"femea-14-arrobas": 0.0,"macho-18-arrobas": 10.0,"femea-16-arrobas": 8.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 15.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}},{"atividade": "PECUARIA_CORTE","modeloProducao": "ENGORDA","areaPastagem": 324.0,"totalAnimais": 1005,"quantidadeVendaBezerro": 0,"precoMedioCabeca": 150.0,"terminacao":{"tipo": "CONFINAMENTO","quantidades":{"macho-14-arrobas": 10.0,"femea-14-arrobas": 10.0,"macho-18-arrobas": 15.0,"femea-16-arrobas": 15.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 10.0},"precoMedioPorArroba": 200.0,"pracaComercializacao": "São Domingos, GO"}}],"pecuariaLeite": [{"atividade": "PECUARIA_LEITE","totalVacas": 90,"produtividadeMedia": 30.0,"precoComercializacao": 10.0,"pracaComercializacao": "São Domingos, GO"}]}}]}';

// Criação e tratamento de JSON E-AGRO
// Criação e tratamento de JSON E-AGRO
function eagro(listAgro) {
    var lista01 = '';
    var lista02 = '';
    var lista03 = '';

    try {
        if (listAgro != '' && listAgro != '##') {
            var jsonArrendamento = JSON.parse(listAgro);

            for (var propriedadesRurais of jsonArrendamento['propriedadesRurais']) {
                
                // Variáveis para Agricola
                var precoComercializacao = '', produtividadeIrrigada = '', areaIrrigada = '';
                var produtividadeSequeira = '', areaSequeira = '', recebimentoArrendamento = '', pagamentoArrendamento = '';
                
                var producaoAgricola = propriedadesRurais['receitaAgropecuaria']['producaoAgricola'];
                for (var i = 0; i < producaoAgricola.length; i++) {
                    if (producaoAgricola[i]['atividade'] === 'AGRICOLA') {
                        precoComercializacao = producaoAgricola[i]['precoComercializacao'] || 0;
                        produtividadeIrrigada = producaoAgricola[i]['produtividadeIrrigada']['produtividade'] || 0;
                        areaIrrigada = producaoAgricola[i]['produtividadeIrrigada']['area'] || 0;
                        produtividadeSequeira = producaoAgricola[i]['produtividadeSequeira']['produtividade'] || 0;
                        areaSequeira = producaoAgricola[i]['produtividadeSequeira']['area'] || 0;
                    }
                }

                recebimentoArrendamento = propriedadesRurais['recebimentoArrendamento'] || 0;
                pagamentoArrendamento = propriedadesRurais['pagamentoArrendamento'] || 0;

                var pecuariaCorte = propriedadesRurais['receitaAgropecuaria']['pecuariaCorte'];
                for (var j = 0; j < pecuariaCorte.length; j++) {
                    var modeloProducao = pecuariaCorte[j]['modeloProducao'] || '';
                    var precoMedioCabeca = pecuariaCorte[j]['precoMedioCabeca'] || 0;
                    var totalAnimais = pecuariaCorte[j]['totalAnimais'] || 0;
                    
                    var quantidades = pecuariaCorte[j]['terminacao']['quantidades'] || {};
                    var macho14 = quantidades['macho-14-arrobas'] || 0;
                    var femea14 = quantidades['femea-14-arrobas'] || 0;
                    var macho18 = quantidades['macho-18-arrobas'] || 0;
                    var femea16 = quantidades['femea-16-arrobas'] || 0;
                    var macho22 = quantidades['macho-22-arrobas'] || 0;
                    var femea20 = quantidades['femea-20-arrobas'] || 0;
                    var precoMedioPorArroba = pecuariaCorte[j]['terminacao']['precoMedioPorArroba'] || 0;

                    // Novas variáveis adicionadas
                    var totalVacas = propriedadesRurais['receitaAgropecuaria']['pecuariaLeite'][0]['totalVacas'] || 0;
                    var produtividadeMedia = propriedadesRurais['receitaAgropecuaria']['pecuariaLeite'][0]['produtividadeMedia'] || 0;
                    var precoComercializacaoLeite = propriedadesRurais['receitaAgropecuaria']['pecuariaLeite'][0]['precoComercializacao'] || 0;

                    // Lista 01: AGRICOLA
                    if (modeloProducao === 'Cria') {
                        lista01 = "AGRICOLA;" + precoComercializacao + ";" + produtividadeIrrigada + ";" + areaIrrigada + ";" + 
                                   produtividadeSequeira + ";" + areaSequeira + ";" + recebimentoArrendamento + ";" + pagamentoArrendamento + ";" + 
                                   modeloProducao + ";" + precoMedioCabeca + ";" + totalAnimais + ";" + macho14 + ";" + femea14 + ";" + 
                                   macho18 + ";" + femea16 + ";" + macho22 + ";" + femea20 + ";" + precoMedioPorArroba + ";" + 
                                   totalVacas + ";" + produtividadeMedia + ";" + precoComercializacaoLeite;
                    }
                    
                    // Lista 02: PECUARIA_CORTE
                    if (modeloProducao === 'ENGORDA') {
                        lista02 = "PECUARIA_CORTE;" + precoComercializacao + ";" + produtividadeIrrigada + ";" + areaIrrigada + ";" + 
                                   produtividadeSequeira + ";" + areaSequeira + ";" + recebimentoArrendamento + ";" + pagamentoArrendamento + ";" + 
                                   modeloProducao + ";" + precoMedioCabeca + ";" + totalAnimais + ";" + macho14 + ";" + femea14 + ";" + 
                                   macho18 + ";" + femea16 + ";" + macho22 + ";" + femea20 + ";" + precoMedioPorArroba + ";" + 
                                   totalVacas + ";" + produtividadeMedia + ";" + precoComercializacaoLeite;
                    }
                }

                // Lista 03: AGRICOLA + PECUARIA concatenados
                lista03 = lista01 + "#" + lista02;
            }
        }
        return lista03;
    } catch (e) {
        return 'Erro ao processar os dados';
    }
}

console.log(eagro(JsonEntrada));





// lista03 = AGRICOLA;80;10.3;10;50;110;50000;0;Cria;100;4000;5;0;10;8;10;15;280;90;30;10
//           PECUARIA_CORTE;80;10.3;10;50;110;50000;0;ENGORDA;150;1005;10;10;15;15;10;10;200;90;30;10


