// copeid from tools.js
/**
 * Converts ioB pattern into regex.
 * @param {string} pattern - Regex string to us it in new RegExp(pattern)
 * @returns {string}
 */
function pattern2RegEx(pattern) {
    pattern = (pattern || '').toString()
        .replace(/\$/g, '\\$')
        .replace(/\^/g, '\\^');

    if (pattern !== '*') {
        if (pattern[0] === '*' && pattern[pattern.length - 1] !== '*') pattern += '$';
        if (pattern[0] !== '*' && pattern[pattern.length - 1] === '*') pattern = '^' + pattern;
        if (pattern[0] !== '*' && pattern[pattern.length - 1] !== '*') pattern = '^' + pattern + '$';
    }

    pattern = pattern
        .replace(/\?/g, '\\?')
        .replace(/\./g, '\\.')
        .replace(/\(/g, '\\(')
        .replace(/\)/g, '\\)')
        .replace(/\[/g, '\\[')
        .replace(/]/g, '\\]')
        .replace(/\*/g, '.*');

    return pattern;
}

/**
 * recursively copy values from old object to new one
 *
 * @alias copyAttributes
 * @memberof tools
 * @param {object} oldObj source object
 * @param {object} newObj destination object
 * @param {object} [originalObj] optional object for read __no_change__ values
 * @param {boolean} [isNonEdit] optional indicator if copy is in nonEdit part
 *
 */
function copyAttributes(oldObj, newObj, originalObj, isNonEdit) {
    for (let attr in oldObj) {
        if (oldObj.hasOwnProperty(attr)) {
            if (typeof oldObj[attr] !== 'object' || oldObj[attr] instanceof Array) {
                if (oldObj[attr] === '__no_change__' && originalObj && !isNonEdit) {
                    if (originalObj[attr] !== undefined) {
                        newObj[attr] = JSON.parse(JSON.stringify(originalObj[attr]));
                    } else {
                        console.log(`Attribute ${attr} ignored by copying`);
                    }
                } else
                if (oldObj[attr] === '__delete__' && !isNonEdit) {
                    if (newObj[attr] !== undefined) {
                        delete newObj[attr];
                    }
                } else {
                    newObj[attr] = oldObj[attr];
                }
            } else {
                newObj[attr] = newObj[attr] || {};
                copyAttributes(oldObj[attr], newObj[attr], originalObj && originalObj[attr], isNonEdit || attr === 'nonEdit');
            }
        }
    }
}

/**
 * Checks the flag nonEdit and restores non-changeable values if required
 *
 * @alias checkNonEditable
 * @memberof tools
 * @param {object} oldObject source object
 * @param {object} newObject destination object
 *
 */
function checkNonEditable(oldObject, newObject) {
    if (!oldObject) return true;
    if (!oldObject.nonEdit && !newObject.nonEdit) return true;

    // if nonEdit is protected with password
    if (oldObject.nonEdit && oldObject.nonEdit.passHash) {
        // If new Object wants to update the nonEdit information
        if (newObject.nonEdit && newObject.nonEdit.password) {
            crypto = crypto || require('crypto');
            const hash = crypto.createHash('sha256').update(newObject.nonEdit.password.toString()).digest('base64');
            if (oldObject.nonEdit.passHash !== hash) {
                delete newObject.nonEdit;
                return false;
            } else {
                oldObject.nonEdit = JSON.parse(JSON.stringify(newObject.nonEdit));
                delete oldObject.nonEdit.password;
                delete newObject.nonEdit.password;
                oldObject.nonEdit.passHash = hash;
                newObject.nonEdit.passHash = hash;
            }
            copyAttributes(newObject.nonEdit, newObject, newObject);

            if (newObject.passHash) delete newObject.passHash;
            if (newObject.nonEdit && newObject.nonEdit.password) delete newObject.nonEdit.password;

            return true;
        } else {
            newObject.nonEdit = oldObject.nonEdit;
        }

    } else if (newObject.nonEdit) {
        oldObject.nonEdit = JSON.parse(JSON.stringify(newObject.nonEdit));
        if (newObject.nonEdit.password) {
            crypto = crypto || require('crypto');
            const hash = crypto.createHash('sha256').update(newObject.nonEdit.password.toString()).digest('base64');
            delete oldObject.nonEdit.password;
            delete newObject.nonEdit.password;
            oldObject.nonEdit.passHash = hash;
            newObject.nonEdit.passHash = hash;
        }
    }

    // restore settings
    copyAttributes(oldObject.nonEdit, newObject, oldObject);

    if (newObject.passHash) delete newObject.passHash;
    if (newObject.nonEdit && newObject.nonEdit.password) delete newObject.nonEdit.password;
    return true;
}
const tools       = {
    checkNonEditable,
    pattern2RegEx
};