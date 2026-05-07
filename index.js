const axios = require('axios');
const fs = require('fs');
const { XMLParser } = require('fast-xml-parser');

const CONFIG = {
    CSV_FIRME_URL: 'https://serv-10.carrefour.com.ar:446/DYN/routes/GCP_DYN_DownloadExport',
    XML_MARKETPLACE_URL: 'https://www.carrefour.com.ar/XMLData/test-dy.xml',
    OUTPUT_FILE: 'feed_unificado.csv'
};

// 👑 FUNCIÓN GOD-TIER (V3): Búsqueda Omnibus (Revisa todas las ubicaciones posibles)
async function getRealInstallments(mktpItems) {
    console.log('🕵️‍♂️ Iniciando escaneo profundo de cuotas reales en VTEX (Búsqueda Omnibus)...');
    const realInstallmentsMap = {};
    const skuIds = [];

    // 1. Extraer los ID de SKU
    for (const item of mktpItems) {
        const match = item.link.match(/idsku=(\d+)/);
        if (match) skuIds.push(match[1]);
    }

    // 2. Agrupar en lotes de 40 para no saturar el servidor
    const chunkSize = 40;
    for (let i = 0; i < skuIds.length; i += chunkSize) {
        const chunk = skuIds.slice(i, i + chunkSize);
        const queryParams = chunk.map(id => `fq=skuId:${id}`).join('&');
        const apiUrl = `https://www.carrefour.com.ar/api/catalog_system/pub/products/search?${queryParams}`;

        try {
            const res = await axios.get(apiUrl);
            
            for (const product of res.data) {
                if (!product.items || !product.items[0]) continue;
                
                const skuId = product.items[0].itemId;
                const sellers = product.items[0].sellers || [];
                const firstSeller = sellers[0] || {};
                const commertialOffer = firstSeller.commertialOffer || {};
                
                let maxCuotasSinInteres = 0;

                // Estrategia 1: Revisar PaymentOptions detallado (Es el más preciso)
                const paymentOptions = commertialOffer.PaymentOptions || {};
                const installmentOptions = paymentOptions.installmentOptions || [];

                for (const option of installmentOptions) {
                    const installments = option.installments || [];
                    for (const inst of installments) {
                        // Verificamos si NO tiene interés y si es más de 1 cuota
                        if ((inst.interestRate === 0 || inst.hasInterestRate === false) && inst.count > 1) {
                            if (inst.count > maxCuotasSinInteres) {
                                maxCuotasSinInteres = inst.count;
                            }
                        }
                    }
                }

                // Estrategia 2: Si la Estrategia 1 falló, revisar el bloque Installments general
                if (maxCuotasSinInteres === 0) {
                     const mainInstallments = commertialOffer.Installments || [];
                     for (const inst of mainInstallments) {
                        if (inst.InterestRate === 0 && inst.NumberOfInstallments > 1) {
                            if (inst.NumberOfInstallments > maxCuotasSinInteres) {
                                maxCuotasSinInteres = inst.NumberOfInstallments;
                            }
                        }
                    }
                }
                
                // Guardamos el resultado en el mapa
                if (maxCuotasSinInteres > 1) {
                    realInstallmentsMap[skuId] = maxCuotasSinInteres;
                }
            }
        } catch (e) {
            console.log(`⚠️ Advertencia leve: No se pudo verificar un lote de la API.`);
        }
        
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

        // Ejecutamos el escáner de cuotas reales
        const realInstallmentsMap = await getRealInstallments(mktpItems);

        const outputStream = fs.createWriteStream(CONFIG.OUTPUT_FILE);

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

        console.log('➕ Agregando productos de Marketplace...');
        for (const item of mktpItems) {
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
    const inStock = item.availability === 'in stock' ? 'true' : 'false'; 
    const brand = item.brand || '';

    let ribbonValue = ''; 
    const match = item.link.match(/idsku=(\d+)/);
    if (match) {
        const skuId = match[1];
        const cuotasReales = realInstallmentsMap[skuId];
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
                if (h.startsWith('lng:carrefourar')) {
                    if (h.endsWith(':price')) return price;
                    if (h.endsWith(':in_stock')) return inStock;
                }
                return '';
        }
    }).join(fileSeparator);
}

run();
