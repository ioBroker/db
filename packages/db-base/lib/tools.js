const path = require('path');
const fs = require('fs');

function getControllerDir() {
    const possibilities = ['iobroker.js-controller', 'ioBroker.js-controller'];
    /** @type {string | null} */
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
        let checkPath = path.join(__dirname, '../..');
        // Also check in the current check dir (along with iobroker.js-controller subdirs)
        possibilities.unshift('');
        outer: while (true) {
            for (const pkg of possibilities) {
                try {
                    const possiblePath = path.join(checkPath, pkg);
                    if (fs.existsSync(path.join(possiblePath, 'lib/tools.js'))) {
                        controllerPath = possiblePath;
                        break outer;
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
        // ??? What is this supposed to do ???
        if (controllerPath && !fs.existsSync(controllerPath)) {
            controllerPath = null;
        }
    } else {
        // ??? What is this supposed to do ???
        controllerPath = path.dirname(controllerPath);
    }
    return controllerPath;
}

module.exports = require(path.join(getControllerDir() || __dirname, 'lib/tools.js'));
