'use strict';

const gulp      = require('gulp');
const obfuscate = require('gulp-javascript-obfuscator');
const del       = require('del');
const fs        = require('fs');

gulp.task('00-clean', () => {
    return del([
        'dist/**/*'
    ]);
});
gulp.task('01-pack', ['00-clean'], done => {
    let objectsInRedis = fs.readFileSync('./lib/objects/objectsInRedis.js').toString('utf8');
    let objectsUtils = fs.readFileSync('./lib/objects/objectsUtils.js').toString('utf8').replace('module.exports = {', 'const utils = {');
    let tools = fs.readFileSync('./lib/objects/tools.js').toString('utf8');

    const scripts = fs.readdirSync(__dirname + '/lib/objects/lua')
        .map(name => {
            return {name: name.replace(/.lua$/, ''), text: fs.readFileSync(__dirname + '/lib/objects/lua/' + name).toString('utf8')};
        })
        .map(script => 'scriptFiles.' + script.name + ' = \'' + script.text.replace(/\r\n|\n/g, '\\n') + '\';')
        .join('\n');

    const lines = objectsInRedis.split('\n');

    for (let l = lines.length - 1; l >= 0; l--) {
        if (lines[l].indexOf('/* @@tools.js@@ */') !== -1) {
            lines[l] = tools + '\n';
        } else if (lines[l].indexOf('/* @@objectsUtils.js@@ */') !== -1) {
            lines[l] = objectsUtils + '\n';
        } else if (lines[l].indexOf("require('./objectsUtils');") !== -1) {
            lines[l] = '';
        } else if (lines[l].indexOf("require('../tools')") !== -1) {
            lines[l] = '';
        }  else if (lines[l].indexOf('@@lua@@') !== -1) {
            lines[l] = scripts;
        }
    }

    if (!fs.existsSync('./dist')) {
        fs.mkdir('./dist');
    }
    fs.writeFileSync('./dist/index.js', lines.join('\n'));
    done();
});

gulp.task('02-obfuscate', ['01-pack'], () =>
    gulp.src('./dist/index.js')
        .pipe(obfuscate({
                compact: true,
                controlFlowFlattening: false,
                deadCodeInjection: false,
                debugProtection: true,
                debugProtectionInterval: true,
                disableConsoleOutput: false,
                identifierNamesGenerator: 'hexadecimal',
                log: false,
                renameGlobals: true,
                rotateStringArray: true,
                selfDefending: true,
                stringArray: true,
                stringArrayEncoding: false,
                stringArrayThreshold: 0.75,
                unicodeEscapeSequence: false
            }
        )).pipe(gulp.dest('./dist'))
);

gulp.task('03-package.json', ['00-clean'], done => {
    if (!fs.existsSync('./dist')) {
        fs.mkdir('./dist');
    }
    const pack = JSON.parse(fs.readFileSync('./package.json').toString('utf8'));
    delete pack.devDependencies;
    delete pack.scripts;
    fs.writeFileSync('./dist/LICENSE', fs.readFileSync('./LICENSE'));
    fs.writeFileSync('./dist/README.md', fs.readFileSync('./README.md'));
    fs.writeFileSync('./dist/package.json', JSON.stringify(pack, null, 2));
    done();
});

gulp.task('default', ['02-obfuscate', '03-package.json']);