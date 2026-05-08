const axios = require('axios');
const fs = require('fs');
const { XMLParser } = require('fast-xml-parser');

const CONFIG = {
    CSV_FIRME_URL: 'https://serv-10.carrefour.com.ar:446/DYN/routes/GCP_DYN_DownloadExport',
    XML_MARKETPLACE_URL: 'https://www.carrefour.com.ar/XMLData/test-dy.xml',
    OUTPUT_FILE: 'feed_unificado.csv',
    // Cuotas que aceptamos como ribbon válido. 3 cuotas se descarta porque es
    // el default "mentiroso" de VTEX que aparece sin ser promo real.
    CUOTAS_VALIDAS: [6, 9, 12, 18]
};

// Helper: GET con reintentos + backoff (para el XML que a veces tira 504)
async function getWithRetry(url, attempt = 1, timeout = 60000) {
    const MAX_ATTEMPTS = 3;
    try {
        const res = await axios.get(url, { timeout });
        return res.data;
    } catch (e) {
        if (attempt < MAX_ATTEMPTS) {
            const wait = 1000 * Math.pow(2, attempt);
            console.log(`  ↻ Reintento ${attempt}/${MAX_ATTEMPTS - 1} (${e.response?.status || e.code || 'error'}) en ${wait}ms...`);
            await new Promise(r => setTimeout(r, wait));
            return getWithRetry(url, attempt + 1, timeout);
        }
        throw e;
    }
}

// Lee las cuotas del XML para un item.
function getCuotasFromXmlItem(item) {
    const installment = item.installment;
    if (!installment) return 0;

    const monthsRaw = installment.months;
    const months = parseInt(String(monthsRaw).trim(), 10);
    if (isNaN(months)) return 0;

    if (!CONFIG.CUOTAS_VALIDAS.includes(months)) return 0;

    return months;
}

async function run() {
    console.log('🚀 Iniciando unificación...');

    try {
        console.log('📥 Descargando XML de Marketplace...');
        const xmlData = await getWithRetry(`${CONFIG.XML_MARKETPLACE_URL}?nocache=${Date.now()}`);
        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(xmlData);
        const mktpItems = jsonObj.DY.channel.item;
        console.log(`✅ ${mktpItems.length} productos de marketplace listos.`);

        const outputStream = fs.createWriteStream(CONFIG.OUTPUT_FILE, { encoding: 'utf8' });
        outputStream.write('\uFEFF'); // BOM para UTF-8

        console.log('📥 Procesando CSV de Firme...');
        const csvRes = await axios({
            method: 'get',
            url: CONFIG.CSV_FIRME_URL,
            responseType: 'stream'
        });

        let headers = [];
        let isFirstLine = true;
        let remainder = '';
        let fileSeparator = ';';

        for await (const chunk of csvRes.data) {
            const lines = (remainder + chunk.toString()).split(/\r?\n/);
            remainder = lines.pop();

            for (let line of lines) {
                if (isFirstLine) {
                    if (line.includes('\t')) fileSeparator = '\t';
                    else if (line.includes(';')) fileSeparator = ';';
                    else if (line.includes(',')) fileSeparator = ',';

                    headers = line.split(fileSeparator).map(h => h.trim());
                    outputStream.write(line + '\n');
                    isFirstLine = false;
                } else {
                    outputStream.write(line + '\n');
                }
            }
        }

        console.log('➕ Agregando productos de Marketplace usando GTIN como identificador...');
        for (const item of mktpItems) {
            const row = buildMktpRow(item, headers, fileSeparator);
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

function buildMktpRow(item, headers, fileSeparator) {
    const price = parseArsPrice(item.sale_price || item.price);
    const inStock = item.availability === 'in stock' ? 'true' : 'false';
    const brand = item.brand || '';
    
    // 🔍 Usamos el GTIN del XML. Si por alguna razón no existiera, 
    // podrías usar item.id como backup, pero aquí asignamos el gtin como pediste.
    const identificador = item.gtin || item.id;

    const cuotas = getCuotasFromXmlItem(item);
    const ribbonValue = cuotas > 0 ? `${cuotas} Cuotas sin interés` : '';

    return headers.map(h => {
        switch (h) {
            // 🆔 Cambio solicitado: sku y group_id ahora llevan el GTIN
            case 'sku': return identificador;
            case 'group_id': return identificador;
            
            case 'name': return `"${item.title.replace(/"/g, '""')}"`;
            case 'url': return item.link;
            case 'image_url': return item.image_link;
            case 'categories': return `"${(item.product_type || 'Marketplace').replace(/ > /g, '|')}"`;
            case 'ribbons': return ribbonValue ? `"${ribbonValue}"` : '';
            case 'keywords': return `"${brand ? brand + ' | ' : ''}Solo envio"`;
            case 'price': return price;
            case 'in_stock': return inStock;
            default:
                if (h.startsWith('lng:carrefourar')) {
                    if (h.endsWith(':price')) return price;
                    if (h.endsWith(':in_stock')) return inStock;
                }
                return '';
        }
    }).join(fileSeparator);
}

run();
