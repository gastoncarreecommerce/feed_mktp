const axios = require('axios');
const fs = require('fs');
const { XMLParser } = require('fast-xml-parser');

const CONFIG = {
    CSV_FIRME_URL: 'https://serv-10.carrefour.com.ar:446/DYN/routes/GCP_DYN_DownloadExport',
    XML_MARKETPLACE_URL: 'https://www.carrefour.com.ar/XMLData/test-dy.xml',
    OUTPUT_FILE: 'feed_unificado.csv'
};

// 👑 FUNCIÓN GOD-TIER (V2): Bypass a la API de VTEX para buscar promos reales sin errores
async function getRealInstallments(mktpItems) {
    console.log('🕵️‍♂️ Iniciando escaneo profundo de cuotas reales en VTEX...');
    const realInstallmentsMap = {};
    const skuIds = [];

    // 1. Extraer los ID de SKU (ej: idsku=277379)
    for (const item of mktpItems) {
        const match = item.link.match(/idsku=(\d+)/);
        if (match) skuIds.push(match[1]);
    }

    // 2. Agrupar en lotes de 40 para no saturar a Carrefour
    const chunkSize = 40;
    for (let i = 0; i < skuIds.length; i += chunkSize) {
        const chunk = skuIds.slice(i, i + chunkSize);
        const queryParams = chunk.map(id => `fq=skuId:${id}`).join('&');
        const apiUrl = `https://www.carrefour.com.ar/api/catalog_system/pub/products/search?${queryParams}`;

        try {
            const res = await axios.get(apiUrl);
            
            // 3. Revisar el JSON interno del checkout
            for (const product of res.data) {
                if (!product.items || !product.items[0]) continue;
                
                const skuId = product.items[0].itemId;
                const sellers = product.items[0].sellers || [];
                const mainInstallments = sellers[0]?.commertialOffer?.Installments || [];
                
                // 4. Buscar la cuota máxima matemática que NO tenga interés (InterestRate = 0)
                let maxCuotasSinInteres = 0;
                
                for (const inst of mainInstallments) {
                    if (inst.InterestRate === 0 && inst.NumberOfInstallments > 1) {
                        if (inst.NumberOfInstallments > maxCuotasSinInteres) {
                            maxCuotasSinInteres = inst.NumberOfInstallments;
                        }
                    }
                }
                
                // Si encontramos cuotas reales, lo guardamos en nuestro diccionario
                if (maxCuotasSinInteres > 1) {
                    realInstallmentsMap[skuId] = maxCuotasSinInteres;
                }
            }
        } catch (e) {
            console.log(`⚠️ Advertencia leve: No se pudo verificar un lote de la API.`);
        }
        
        // Pausa de 300 milisegundos entre lote y lote para ser indetectables
        await new Promise(r => setTimeout(r, 300)); 
    }
    
    console.log(`✅ Escaneo completo. ¡Se descubrieron cuotas reales en ${Object.keys(realInstallmentsMap).length} productos!`);
    return realInstallmentsMap;
}

async function run() {
    console.log('🚀 Iniciando unificación...');

    try {
        console.log('📥 Descargando XML de Marketplace...');
        const xmlRes = await axios.get(`${CONFIG.XML_MARKETPLACE_URL}?nocache=${Date.now()}`);
        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(xmlRes.data);
        const mktpItems = jsonObj.DY.channel.item;
        console.log(`✅ ${mktpItems.length} productos de marketplace listos.`);

        // 👑 Ejecutamos nuestro escáner profundo de cuotas
        const realInstallmentsMap = await getRealInstallments(mktpItems);

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
                    // Dejamos pasar la línea intacta (mantiene los true/false originales de DY)
                    outputStream.write(line + '\n');
                }
            }
        }

        console.log('➕ Agregando productos de Marketplace al final...');
        for (const item of mktpItems) {
            // Le pasamos el diccionario de cuotas a la constructora de la fila
            const row = buildMktpRow(item, headers, fileSeparator, realInstallmentsMap);
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

function buildMktpRow(item, headers, fileSeparator, realInstallmentsMap) {
    const price = parseArsPrice(item.sale_price || item.price);
    // Acá le mandamos el texto 'true' y 'false' como exige DY
    const inStock = item.availability === 'in stock' ? 'true' : 'false'; 
    const brand = item.brand || '';

    let ribbonValue = ''; 
    // 👑 Consultamos nuestro diccionario en lugar de creerle al XML mentiroso
    const match = item.link.match(/idsku=(\d+)/);
    if (match) {
        const skuId = match[1];
        const cuotasReales = realInstallmentsMap[skuId];
        // Si la API confirmó que tiene cuotas sin interés, armamos el cartelito
        if (cuotasReales) {
            ribbonValue = `${cuotasReales} Cuotas sin interés`;
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
