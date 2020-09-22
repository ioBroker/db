const path = require('path');
const fs = require('fs');

function getControllerDir() {
    const possibilities = ['iobroker.js-controller', 'ioBroker.js-controller'];
    let controllerPath = null;
    for (const pkg of possibilities) {
        try {
            const possiblePath = require.resolve(pkg);
            if (fs.existsSync(possiblePath)) {
                controllerPath = possiblePath;
                break;
            }
        } catch (_a) {
            /* not found */
        }
    }
    // Apparently, checking vs null/undefined may miss the odd case of controllerPath being ""
    // Thus we check for falsyness, which includes failing on an empty path
    if (!controllerPath) {
        const checkPath = path.normalize(path.join(__dirname, '../..'));
        const pathParts = checkPath.split(path.sep);
        while (pathParts.length) {
            const tryPath = path.join(path.sep, pathParts.join(path.sep));
            if (fs.existsSync(path.join(tryPath, 'lib/tools.js'))) {
                controllerPath = tryPath;
                break;
            }
            // Mainly for local development cases
            if (fs.existsSync(path.join(tryPath, 'iobroker.js-controller/lib/tools.js'))) {
                controllerPath = path.join(tryPath, 'iobroker.js-controller');
                break;
            }
            if (fs.existsSync(path.join(tryPath, 'ioBroker.js-controller/lib/tools.js'))) {
                controllerPath = path.join(tryPath, 'ioBroker.js-controller');
                break;
            }
            pathParts.pop();
        }
        if (controllerPath && !fs.existsSync(controllerPath)) {
            controllerPath = null;
        }
    } else {
        controllerPath = path.dirname(controllerPath);
    }
    return controllerPath;
}

module.exports = require(path.join(getControllerDir() || __dirname, 'lib/tools.js'));
