// copied from tools.js
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

let crypto_;

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
    Object.keys(oldObj).forEach(attr => {
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
    });
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
    if (!oldObject) {
        return true;
    } else
    if (!oldObject.nonEdit && !newObject.nonEdit) {
        return true;
    }

    // if nonEdit is protected with password
    if (oldObject.nonEdit && oldObject.nonEdit.passHash) {
        // If new Object wants to update the nonEdit information
        if (newObject.nonEdit && newObject.nonEdit.password) {
            crypto_ = crypto_ || require('crypto');
            const hash = crypto_.createHash('sha256').update(newObject.nonEdit.password.toString()).digest('base64');
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

            if (newObject.passHash) {
                delete newObject.passHash;
            }
            if (newObject.nonEdit && newObject.nonEdit.password) {
                delete newObject.nonEdit.password;
            }

            return true;
        } else {
            newObject.nonEdit = oldObject.nonEdit;
        }

    } else if (newObject.nonEdit) {
        oldObject.nonEdit = JSON.parse(JSON.stringify(newObject.nonEdit));
        if (newObject.nonEdit.password) {
            crypto_ = crypto_ || require('crypto');
            const hash = crypto_.createHash('sha256').update(newObject.nonEdit.password.toString()).digest('base64');
            delete oldObject.nonEdit.password;
            delete newObject.nonEdit.password;
            oldObject.nonEdit.passHash = hash;
            newObject.nonEdit.passHash = hash;
        }
    }

    // restore settings
    copyAttributes(oldObject.nonEdit, newObject, oldObject);

    if (newObject.passHash) {
        delete newObject.passHash;
    }
    if (newObject.nonEdit && newObject.nonEdit.password) {
        delete newObject.nonEdit.password;
    }
    return true;
}

/**
 * Checks if the given callback is a function and if so calls it with the given parameter immediately, else a resolved Promise is returned
 *
 * @param {(...args: any[]) => void | null | undefined} callback - callback function to be executed
 * @param {any[]} args - as many arguments as needed, which will be returned by the callback function or by the Promise
 * @returns {Promise<any>} - if Promise is resolved with multiple arguments, an array is returned
 */
function maybeCallback(callback, ...args) {
    if (typeof callback === 'function') {
        // if function we call it with given param
        setImmediate(callback, ...args);
    } else {
        return Promise.resolve(args.length > 1 ? args : args[0]);
    }
}

/**
 * Checks if the given callback is a function and if so calls it with the given error and parameter immediately, else a resolved or rejected Promise is returned
 *
 * @param {((error: Error, ...args: any[]) => void) | null | undefined} callback - callback function to be executed
 * @param {Error | string | null | undefined} error - error which will be used by the callback function. If callback is not a function and
 * error is given, a rejected Promise is returned. If error is given but it is not an instance of Error, it is converted into one.
 * @param {any[]} args - as many arguments as needed, which will be returned by the callback function or by the Promise
 * @returns {Promise<any>} - if Promise is resolved with multiple arguments, an array is returned
 */
function maybeCallbackWithError(callback, error, ...args) {
    if (error !== undefined && error !== null && !(error instanceof Error)) {
        // if its not a real Error, we convert it into one
        error = new Error(error);
    }

    if (typeof callback === 'function') {
        setImmediate(callback, error, ...args);
    } else if (error) {
        return Promise.reject(error);
    } else {
        return Promise.resolve(args.length > 1 ? args : args[0]);
    }
}

const tools       = {
    checkNonEditable,
    pattern2RegEx,
    maybeCallback,
    maybeCallbackWithError
};
