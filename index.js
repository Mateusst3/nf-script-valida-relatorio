const fs = require('fs');
const csv = require('csv-parser');

function lerColunasSelecionadas(caminho, indicesColunas) {
  return new Promise((resolve, reject) => {
    const resultados = [];
    let nomesColunas = [];

    fs.createReadStream(caminho)
      .pipe(csv())
      .on('headers', (headers) => {
        nomesColunas = indicesColunas.map(i => {
          if (i >= headers.length) {
            reject(new Error(`Arquivo ${caminho} não possui coluna na posição ${i + 1}`));
          }
          return headers[i];
        });
      })
      .on('data', (linha) => {
        const dados = nomesColunas.map(nome => linha[nome]);
        resultados.push(dados);
      })
      .on('end', () => resolve(resultados))
      .on('error', reject);
  });
}

function compararColunas(linhas1, linhas2, nomes) {
  const inconsistencias = [];
  const max = Math.max(linhas1.length, linhas2.length);

  for (let i = 0; i < max; i++) {
    const linha1 = linhas1[i] || [];
    const linha2 = linhas2[i] || [];

    for (let j = 0; j < nomes.length; j++) {
      const valor1 = linha1[j] ?? '[vazio]';
      const valor2 = linha2[j] ?? '[vazio]';

      if (valor1 !== valor2) {
        inconsistencias.push(
          `Linha ${i + 1}, coluna ${nomes[j]}: "${valor1}" vs "${valor2}"`
        );
      }
    }
  }

  return inconsistencias;
}

async function main() {
  const caminho1 = 'tabela1.csv';
  const caminho2 = 'tabela2.csv';

  const indicesDesejados = [1, 3, 4];

  try {
    const dados1 = await lerColunasSelecionadas(caminho1, indicesDesejados);
    const dados2 = await lerColunasSelecionadas(caminho2, indicesDesejados);

    const stream = fs.createReadStream(caminho1).pipe(csv());
    let nomes = [];

    await new Promise((resolve, reject) => {
      stream.on('headers', headers => {
        nomes = indicesDesejados.map(i => headers[i]);
        resolve();
      });
      stream.on('error', reject);
    });

    const inconsistencias = compararColunas(dados1, dados2, nomes);

    if (inconsistencias.length === 0) {
      console.log('✔️ As colunas B, D e E são idênticas.');
    } else {
      console.log('❌ Inconsistências encontradas nas colunas B, D e E:');
      inconsistencias.forEach(msg => console.log(msg));
    }
  } catch (erro) {
    console.error('Erro:', erro.message);
  }
}

main();
