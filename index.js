const axios = require('axios');
const fs = require('fs');
const { XMLParser } = require('fast-xml-parser');

const CONFIG = {
    CSV_FIRME_URL: 'https://serv-10.carrefour.com.ar:446/DYN/routes/GCP_DYN_DownloadExport',
    XML_MARKETPLACE_URL: 'https://www.carrefour.com.ar/XMLData/test-dy.xml',
    OUTPUT_FILE: 'feed_unificado.csv',
    SEPARATOR: ';'
};

async function run() {
    console.log('🚀 Iniciando unificación...');

    try {
        // 1. Descargar y parsear XML de Marketplace (es chico, entra en memoria)
        console.log('📥 Descargando XML de Marketplace...');
        const xmlRes = await axios.get(CONFIG.XML_MARKETPLACE_URL);
        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(xmlRes.data);
        const mktpItems = jsonObj.rss.channel.item;
        console.log(`✅ ${mktpItems.length} productos de marketplace listos.`);

        // 2. Preparar el Stream de salida
        const outputStream = fs.createWriteStream(CONFIG.OUTPUT_FILE);

        // 3. Descargar CSV de Firme y procesar línea por línea (Streaming)
        console.log('📥 Procesando CSV de Firme (86k filas)...');
        const csvRes = await axios({
            method: 'get',
            url: CONFIG.CSV_FIRME_URL,
            responseType: 'stream'
        });

        let headers = [];
        let isFirstLine = true;
        let remainder = '';

        // Procesador de chunks para no saturar la memoria
        for await (const chunk of csvRes.data) {
            const lines = (remainder + chunk.toString()).split(/\r?\n/);
            remainder = lines.pop(); // Guardar línea incompleta para el próximo chunk

            for (let line of lines) {
                if (isFirstLine) {
                    headers = line.split(CONFIG.SEPARATOR).map(h => h.trim());
                    outputStream.write(line + '\n');
                    isFirstLine = false;
                } else {
                    // Optimización de peso: false->0, true->1
                    let optimizedLine = line.replace(/;false(?=;|\r|\n|$)/gi, ';0')
                                            .replace(/;true(?=;|\r|\n|$)/gi, ';1');
                    outputStream.write(optimizedLine + '\n');
                }
            }
        }

        // 4. Transformar y concatenar Marketplace al final
        console.log('➕ Agregando productos de Marketplace al final...');
        for (const item of mktpItems) {
            const row = buildMktpRow(item, headers);
            outputStream.write(row + '\n');
        }

        outputStream.end();
        console.log('✨ Proceso finalizado con éxito.');

    } catch (err) {
        console.error('❌ Error crítico:', err.message);
        process.exit(1);
    }
}

function parseArsPrice(p) {
    if (!p) return "0.00";
    const cleaned = String(p).replace(/ARS\s*/gi, '').replace(/\./g, '').replace(',', '.').replace(/[^0-9.-]+/g, '');
    return parseFloat(cleaned).toFixed(2);
}

function buildMktpRow(item, headers) {
    const price = parseArsPrice(item.sale_price || item.price);
    const inStock = item.availability === 'in stock' ? '1' : '0';
    const brand = item.brand || '';

    return headers.map(h => {
        switch (h) {
            case 'sku': return item.id;
            case 'group_id': return item.id;
            case 'name': return `"${item.title.replace(/"/g, '""')}"`;
            case 'url': return item.link;
            case 'image_url': return item.image_link;
            case 'categories': return `"${(item.product_type || 'Marketplace').replace(/ > /g, '|')}"`;
            case 'ribbons': return 'Marketplace';
            case 'keywords': return `"${brand}"`;
            case 'price': return price;
            case 'in_stock': return inStock;
            default:
                if (h.startsWith('lng:carrefourar')) {
                    if (h.endsWith(':price')) return price;
                    if (h.endsWith(':in_stock')) return inStock;
                }
                return '';
        }
    }).join(CONFIG.SEPARATOR);
}

run();
