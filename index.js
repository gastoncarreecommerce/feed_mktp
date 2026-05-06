const axios = require('axios');
const fs = require('fs');
const { XMLParser } = require('fast-xml-parser');

const CONFIG = {
    CSV_FIRME_URL: 'https://serv-10.carrefour.com.ar:446/DYN/routes/GCP_DYN_DownloadExport',
    XML_MARKETPLACE_URL: 'https://www.carrefour.com.ar/XMLData/test-dy.xml',
    OUTPUT_FILE: 'feed_unificado.csv'
};

async function run() {
    console.log('🚀 Iniciando unificación...');

    try {
        console.log('📥 Descargando XML de Marketplace...');
        const xmlRes = await axios.get(`${CONFIG.XML_MARKETPLACE_URL}?nocache=${Date.now()}`);
        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(xmlRes.data);
        const mktpItems = jsonObj.DY.channel.item;
        console.log(`✅ ${mktpItems.length} productos de marketplace listos.`);

        const outputStream = fs.createWriteStream(CONFIG.OUTPUT_FILE);

        console.log('📥 Procesando CSV de Firme (86k filas)...');
        const csvRes = await axios({
            method: 'get',
            url: CONFIG.CSV_FIRME_URL,
            responseType: 'stream'
        });

        let headers = [];
        let isFirstLine = true;
        let remainder = '';
        let fileSeparator = ';'; // Separador por defecto

        for await (const chunk of csvRes.data) {
            const lines = (remainder + chunk.toString()).split(/\r?\n/);
            remainder = lines.pop(); 

            for (let line of lines) {
                if (isFirstLine) {
                    // 💡 Detector automático de separador
                    if (line.includes('\t')) fileSeparator = '\t';
                    else if (line.includes(';')) fileSeparator = ';';
                    else if (line.includes(',')) fileSeparator = ',';
                    
                    headers = line.split(fileSeparator).map(h => h.trim());
                    outputStream.write(line + '\n');
                    isFirstLine = false;
                } else {
                    // Dejamos pasar la línea intacta (mantiene los true/false originales)
                    outputStream.write(line + '\n');
                }
            }
        }

        console.log('➕ Agregando productos de Marketplace al final...');
        for (const item of mktpItems) {
            // Le pasamos el separador detectado para que no se desalinee
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
    // Acá le mandamos el texto 'true' y 'false' como exige DY
    const inStock = item.availability === 'in stock' ? 'true' : 'false'; 
    const brand = item.brand || '';

    let ribbonValue = ''; 
    if (item.installment) {
        let cuotas = item.installment.months || item.installment; 
        if (cuotas && !isNaN(cuotas) && parseInt(cuotas) > 1) {
            ribbonValue = `${cuotas} Cuotas sin interés`;
        }
    }

    return headers.map(h => {
        switch (h) {
            case 'sku': return item.id;
            case 'group_id': return item.id;
            case 'name': return `"${item.title.replace(/"/g, '""')}"`;
            case 'url': return item.link;
            case 'image_url': return item.image_link;
            case 'categories': return `"${(item.product_type || 'Marketplace').replace(/ > /g, '|')}"`;
            case 'ribbons': return ribbonValue ? `"${ribbonValue}"` : '';
            case 'keywords': return `"${brand}"`;
            case 'price': return price;
            case 'in_stock': return inStock;
            default:
                // Duplicamos precio y stock en las columnas de las sucursales
                if (h.startsWith('lng:carrefourar')) {
                    if (h.endsWith(':price')) return price;
                    if (h.endsWith(':in_stock')) return inStock;
                }
                return '';
        }
    }).join(fileSeparator);
}

run();
