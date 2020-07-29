'use strict';

const gulp       = require('gulp');
const del        = require('del');
const fs         = require('fs');

gulp.task('00-clean', () =>
    del([
        'dist/**/*'
    ])
);

gulp.task('01-pack', gulp.series('00-clean', done => {
    let objectsInRedis = fs.readFileSync('./lib/objects/objectsInRedis.js').toString('utf8');
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
        } else if (lines[l].indexOf("require('../tools')") !== -1) {
            lines[l] = '';
        }  else if (lines[l].indexOf('@@lua@@') !== -1) {
            lines[l] = scripts;
        }
    }

    if (!fs.existsSync('./dist')) {
        fs.mkdirSync('./dist');
    }
    fs.writeFileSync('./dist/index.js', lines.join('\n'));
    fs.writeFileSync('./dist/.npmignore', '*.js.map');

    if (!fs.existsSync('./lookup')) {
        fs.mkdirSync('./lookup');
    }
    const pack = JSON.parse(fs.readFileSync('./package.json').toString('utf8'));
    if (!fs.existsSync('./lookup/' + pack.version)) {
        fs.mkdirSync('./lookup/' + pack.version);
    }

    fs.writeFileSync('./lookup/' + pack.version + '/index.js', lines.join('\n'));
    done();
}));

gulp.task('02-package.json', done => {
    if (!fs.existsSync('./dist')) {
        fs.mkdir('./dist');
    }
    const pack = JSON.parse(fs.readFileSync('./package.json').toString('utf8'));
    delete pack.devDependencies;
    delete pack.scripts;
    fs.writeFileSync('./dist/CHANGELOG_OLD.md', fs.readFileSync('./CHANGELOG_OLD.md')); // is it really needed
    fs.writeFileSync('./dist/LICENSE', fs.readFileSync('./LICENSE'));
    fs.writeFileSync('./dist/README.md', fs.readFileSync('./README.md'));
    fs.writeFileSync('./dist/package.json', JSON.stringify(pack, null, 2));
    done();
});

gulp.task('default', gulp.series('01-pack', '02-package.json'));