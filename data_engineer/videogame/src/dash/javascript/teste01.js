
JsonEntrada = '{"numeroCPFCNPJ": "11923474634","linhaCredito": "CPR_CUSTEIOS","propriedadesRurais": [{"numeroCAR": "GO5219803C891C6187E7145CD8AFA6EF1A0236642","numeroMatricula": "1689","caracteristicaPropriedade": "IMOVEL_MEU_E_OUTROS","formaAtuacao": "OUTROS","areaTotalImovel": 29473367.0,"recebimentoArrendamento": 50000.0,"pagamentoArrendamento": 0.0,"dataFinalContrato": "2024-08-31T02:40:26.827+00:00","receitaAgropecuaria": {"producaoAgricola": [{"atividade": "AGRICOLA","produto": "SOJA","variedade": "SOJA PEQUENA","safra": "SAFRA 1","especificacao": "SACO 60KG","precoComercializacao": 80.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.3,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}},{"atividade": "PECUARIA_CORTE","produto": "CANA-DE-ACUCAR","variedade": "CANA","safra": "SAFRA 1","especificacao": "SACO 60KG","precoComercializacao": 95.0,"pracaComercializacao": "São Domingos, GO","produtividadeIrrigada": {"area": 10.0,"produtividade": 10.0,"unidadeMedida": "ha"},"produtividadeSequeira": {"area": 110.0,"produtividade": 50.0,"unidadeMedida": "ha"}}],"pecuariaCorte": [{"atividade": "PECUARIA_CORTE","modeloProducao": "Cria","areaPastagem": 110.0,"totalAnimais": 4000,"quantidadeVendaBezerro": 40,"precoMedioCabeca": 100.0,"terminacao":{"tipo": "Confinamento","quantidades": {"macho-14-arrobas": 5.0,"femea-14-arrobas": 0.0,"macho-18-arrobas": 10.0,"femea-16-arrobas": 8.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 15.0},"precoMedioPorArroba": 280.0,"pracaComercializacao": "São Domingos, GO"}},{"atividade": "PECUARIA_CORTE","modeloProducao": "ENGORDA","areaPastagem": 324.0,"totalAnimais": 1005,"quantidadeVendaBezerro": 0,"precoMedioCabeca": 150.0,"terminacao":{"tipo": "CONFINAMENTO","quantidades":{"macho-14-arrobas": 10.0,"femea-14-arrobas": 10.0,"macho-18-arrobas": 15.0,"femea-16-arrobas": 15.0,"macho-22-arrobas": 10.0,"femea-20-arrobas": 10.0},"precoMedioPorArroba": 200.0,"pracaComercializacao": "São Domingos, GO"}}],"pecuariaLeite": [{"atividade": "PECUARIA_LEITE","totalVacas": 90,"produtividadeMedia": 30.0,"precoComercializacao": 10.0,"pracaComercializacao": "São Domingos, GO"}]}}]}';

function eagro(listAgro) {
    var resultado = [];

    try {
        if (listAgro && listAgro !== '##') {
            var jsonArrendamento = JSON.parse(listAgro);

            for (var propriedadesRurais of jsonArrendamento['propriedadesRurais']) {
                var recebimentoArrendamento = propriedadesRurais['recebimentoArrendamento'] || 0;
                var pagamentoArrendamento = propriedadesRurais['pagamentoArrendamento'] || 0;

                var producaoAgricola = propriedadesRurais['receitaAgropecuaria']['producaoAgricola'];
                for (var i = 0; i < producaoAgricola.length; i++) {
                    var atividade = producaoAgricola[i]['atividade'] || '';
                    var precoComercializacao = producaoAgricola[i]['precoComercializacao'] || 0;
                    var produtividadeIrrigada = producaoAgricola[i]['produtividadeIrrigada']['produtividade'] || 0;
                    var areaIrrigada = producaoAgricola[i]['produtividadeIrrigada']['area'] || 0;
                    var produtividadeSequeira = producaoAgricola[i]['produtividadeSequeira']['produtividade'] || 0;
                    var areaSequeira = producaoAgricola[i]['produtividadeSequeira']['area'] || 0;

                    var pecuariaCorte = propriedadesRurais['receitaAgropecuaria']['pecuariaCorte'][i] || {};
                    var modeloProducao = pecuariaCorte['modeloProducao'] || '';
                    var precoMedioCabeca = pecuariaCorte['precoMedioCabeca'] || 0;
                    var totalAnimais = pecuariaCorte['quantidadeVendaBezerro'] || 0;

                    var quantidades = pecuariaCorte['terminacao'] ? pecuariaCorte['terminacao']['quantidades'] : {};
                    var macho14 = quantidades['macho-14-arrobas'] || 0;
                    var femea14 = quantidades['femea-14-arrobas'] || 0;
                    var macho18 = quantidades['macho-18-arrobas'] || 0;
                    var femea16 = quantidades['femea-16-arrobas'] || 0;
                    var macho22 = quantidades['macho-22-arrobas'] || 0;
                    var femea20 = quantidades['femea-20-arrobas'] || 0;
                    var precoMedioPorArroba = pecuariaCorte['terminacao'] ? pecuariaCorte['terminacao']['precoMedioPorArroba'] : 0;

                    var totalVacas = propriedadesRurais['receitaAgropecuaria']['pecuariaLeite'][0]['totalVacas'] || 0;
                    var produtividadeMedia = propriedadesRurais['receitaAgropecuaria']['pecuariaLeite'][0]['produtividadeMedia'] || 0;
                    var precoComercializacaoLeite = propriedadesRurais['receitaAgropecuaria']['pecuariaLeite'][0]['precoComercializacao'] || 0;

                    resultado.push(
                        atividade + ";" + precoComercializacao + ";" + produtividadeIrrigada + ";" + areaIrrigada + ";" +
                        produtividadeSequeira + ";" + areaSequeira + ";" + recebimentoArrendamento + ";" + pagamentoArrendamento + ";" +
                        modeloProducao + ";" + precoMedioCabeca + ";" + totalAnimais + ";" + macho14 + ";" + femea14 + ";" +
                        macho18 + ";" + femea16 + ";" + macho22 + ";" + femea20 + ";" + precoMedioPorArroba + ";" +
                        totalVacas + ";" + produtividadeMedia + ";" + precoComercializacaoLeite
                    );
                }
            }
        }
        return resultado.join('#');
    } catch (e) {
        return 'Erro ao processar os dados';
    }

}
console.log(eagro(JsonEntrada));

