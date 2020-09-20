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
        controllerPath = path.join(__dirname, '..', '..', '..', '..', 'ioBroker.js-controller');
        if (!fs.existsSync(controllerPath)) {
            controllerPath = null;
        }
    } else {
        controllerPath = path.dirname(controllerPath);
    }
    return controllerPath;
}

module.exports = require(path.join(getControllerDir() || __dirname, 'lib/tools.js'))