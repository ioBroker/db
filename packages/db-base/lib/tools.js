const path = require('path');
const fs = require('fs');

function getControllerDir() {
    const possibilities = ['iobroker.js-controller', 'ioBroker.js-controller'];
    for (const pkg of possibilities) {
        try {
            // package.json is guaranteed to be in the module root folder
            // so once that is resolved, take the dirname and we're done
            const possiblePath = require.resolve(`${pkg}/package.json`);
            if (fs.existsSync(possiblePath)) {
                return path.dirname(possiblePath);
            }
        } catch (_a) {
            /* not found */
        }
    }

    // Apparently, checking vs null/undefined may miss the odd case of controllerPath being ""
    // Thus we check for falsyness, which includes failing on an empty path
    let checkPath = path.join(__dirname, '../..');
    // Also check in the current check dir (along with iobroker.js-controller subdirs)
    possibilities.unshift('');
    while (true) {
        for (const pkg of possibilities) {
            try {
                const possiblePath = path.join(checkPath, pkg);
                if (fs.existsSync(path.join(possiblePath, 'lib/tools.js'))) {
                    return possiblePath;
                }
            } catch (_a) {
                // not found, continue with next possiblity
            }
        }

        // Controller not found here - go to the parent dir
        const newPath = path.dirname(checkPath);
        if (newPath === checkPath) {
            // We already reached the root dir, abort
            break;
        }
        checkPath = newPath;
    }
}

module.exports = require(path.join(getControllerDir() || __dirname, 'lib/tools.js'));
