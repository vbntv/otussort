const fs = require('fs');
const readline = require('readline');

const filepath = 'numbers.txt';
const sortedFilePath = 'sorted_' + filepath;
const tempPath = 'temp/';
const targetSize = 100000; // 100 mb
const minNumber = 1;
const maxNumber = 9999999997;

function bytesToKBytes(bytes) {
    return bytes / 1024;
}

function generateChunkOfNumbers() {
    let numbers = [];
    for (let i = 0; i < 1000; i++) {
        numbers.push(Math.floor(Math.random() * (maxNumber - minNumber + 1) + minNumber));
    }
    return numbers;
}

function divToSortedFiles(filepath) {
    const rr = fs.createReadStream(filepath, {highWaterMark: 1024});
    const rl = readline.createInterface({input: rr});
    const subFileSize = fs.statSync(filepath).size / 20;
    let subFilesPaths = [];
    let i = 1;
    let lines = [];
    let size = 0;
    rl.on('line', (line) => {
        lines.push(line);
        size += (line.toString().length + 1);
        if (size > (subFileSize)) {
            lines.sort((a, b) => a - b);
            let path = `${tempPath}${i}_chunk_${filepath}`;
            subFilesPaths.push(path);
            fs.writeFileSync(path, lines.join('\n'));
            i++;
            lines = [];
            size = 0;
        }
    })

    rr.on('end', function () {
        lines.sort((a, b) => a - b);
        let path = `${tempPath}${i}_chunk_${filepath}`;
        fs.writeFileSync(path, lines.join('\n'));
        subFilesPaths.push(path);
    })

    return { rr: rr, subFilesPaths: subFilesPaths};
}

function generateFile(filepath) {
    let file = fs.openSync(filepath, 'w');
    let currentsSize = 0;
    do {
        fs.writeSync(file, generateChunkOfNumbers().join('\n') + '\n');
        currentsSize = bytesToKBytes(fs.statSync(filepath).size);
    }
    while (currentsSize < targetSize);
}

function getStreamIterators(paths) {
    let iterators = [];
    paths.forEach((path) => {
        iterators.push(readline.createInterface({
            input: fs.createReadStream(path, {highWaterMark: 1024})
        })[Symbol.asyncIterator]())});
    return iterators;
}

async function main () {
    console.log('Generating file...');
    generateFile(filepath);

    console.log('Dividing to subfiles and sorting...');
    let { rr, subFilesPaths } = divToSortedFiles(filepath);

    rr.on('end', async function () {
        console.log('Merging...');
        const outStream = fs.createWriteStream(sortedFilePath, {highWaterMark: 1024});
        let numbers = [];
        let streamIterators = getStreamIterators(subFilesPaths);

        for (const streamIterator of streamIterators) {
            let number = await streamIterator.next();
            numbers.push(number.value);
        }

        let min = Math.min(...numbers);

        while (streamIterators.length > 0) {
            let idxOfMin = numbers.findIndex(
                (number) => number == min
            );
            let  buf = await streamIterators[idxOfMin].next();
            numbers[idxOfMin] = buf.value;
            if (buf.done) {
                streamIterators.splice(idxOfMin, 1);
                numbers.splice(idxOfMin, 1);
            }
            min = Math.min(...numbers);
            outStream.write(min + '\n');
        }

        console.log('Deleting temporary files...');
        for (const path of subFilesPaths) {
            fs.unlinkSync(path);
        }

        console.log(`Done. Unsorted file: ${filepath}. Sorted file: ${sortedFilePath}`);
    })
}

try {
   main();
} catch (exception) {
    console.log(exception)
}



