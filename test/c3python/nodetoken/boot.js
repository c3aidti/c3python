/*
 * Copyright 2009-2018 C3, Inc. dba C3 IoT (http://www.c3iot.ai)
 * This material, including without limitation any software, is the confidential trade secret
 * and proprietary information of C3 IoT and its licensors. Reproduction, use and distribution
 * of this material in any form is strictly prohibited except as set forth in a written
 * license agreement with C3 IoT and/or its authorized distributors.
 */

if (typeof getFile != 'function') {
  console.error("\nPlease run install again. The base application is no longer compatible.\n");
  process.exit(1);
}

// lib here is lib from c3.js
lib.crypto = require('crypto');
lib.chalk = require('chalk');
lib.ora = require('ora');
lib.md = require('ansimd');

var helpMsg = ' Use --help for usage information.';

// calls the addOpts function in c3.js to add options
addOpts({
  version: opts.version || opts.V,
  adminUser: opts.user || opts.u,
  adminPass: opts.password || opts.p,
  authToken: opts["auth-token"] || opts.T,
  keyFile: opts.key || opts.k,
  tenant: opts.tenant || opts.t,
  tag: opts.tag || opts.g,
  app: opts.app || opts.A, //where -A is in script as option -> node or prov
  locale: opts.locale || opts.l,
});

opts.isBasicAuth = false;
opts.isC3Auth = false;
opts.isC3KeyAuth = false;

function printVersion() {
  if(opts.version) {
    // Getting the spinner to spin is hard. This function can only be executed after the JS typesystem has been loaded
    // since only then can it call c3ImportAll(). Normally one would call setTimeout() or setInterval() to make the
    // spinner spin. However this executes asynchronously and would call process.exit(0) before executing the spinner and end
    // the session. If I remove the process.exit(0) then it calls the apps files in /cli while I wait for the spinner to spin
    // which I don't want it to do.

    var spinner = lib.ora("Fetching server version ...").start();
    c3ImportAll();
    var serverVersion = C3.type.ServerInfo.getInfo().details.buildCITag;
    if (serverVersion != null) {
      serverVersion = serverVersion.split("+")[0].split('-')[0];
    }
    if(serverVersion) {
      spinner.succeed("you are on c3server " + opts.url);
      lib.log.info(serverVersion);
    } else {
      spinner.fail("Failed to fetch server version on " + opts.url);
    }

    process.exit(0);
  }
}

//pvtkey is used to generate c3key auth token for long running processes
opts.pvtKey;

module.exports.helpFiles = {};

// _ REPL/Typesystem/Apps code need _
global._ = require('underscore');
// Eval'ed code needs access to require
global.require = require;

/**
 * This is only called for loading JS type system
 */
function _evalStatic(text, fileName) {
  /* jshint evil:true */
  var scope = {};
  eval.call(scope,text);
  /* jshint evil:false */

  var re = new RegExp("^c3[A-Z]");
  var c3Globals = Object.keys(scope).filter(function (k) { return k.match(re) != null;});
  c3Globals.forEach(function (g) {
    if (typeof g == 'function') {
      module.exports[g.substr(2,1).toLowerCase()+g.substr(3)] = g;
      global[g] = g;
    }
  });
}

/**
 * Loads JS type system where exports = True. Then loads apps manifest.json where exports = false
 */
function _loadFile(path, exports) {
  return function(callback) {
    getFile(path, function (body) {
      var ques = path.lastIndexOf("?");
      ques = ques > -1 ? ques : path.length;
      var ext = path.substring(path.lastIndexOf(".") + 1, ques );
      var fileName = path.substring(path.lastIndexOf("/") + 1, ques);
      switch (ext) {
        case "js":
          if(exports) {
            _evalStatic(body,fileName);
          }
          else {
            /* jshint evil:true */
            eval(body);
            /* jshint evil:false */
          }
          if (callback)
            callback();
          break;
        case "json":
          if (fileName == "manifest.json") {
            var manifest;
            try {
              manifest = JSON.parse(body.replace(/\w*\/\/.*/g, ''));
            } catch (e) {
              lib.log.error("Error: cannot parse manifest.json received from the server");
              process.exit(1);
            }
            var appFileNames = manifest.appFiles;
            if (!appFileNames) {
              lib.log.error("Error: cannot determine application files from manifest.json");
              process.exit(1);
            }
            var tasks = [];
            var appUrl = '/static/nodejs-apps/' + opts.app + "/src/";
            for (var i = 0; i < appFileNames.length; i++) {
              tasks.push(_loadFile(appUrl + appFileNames[i], false)); //loads apps files from manifest.json
            }

            var helpFileNames = manifest.helpFiles;
            if (helpFileNames) {
              var helpUrl = '/static/nodejs-apps/' + opts.app + "/docs/";
              for (i = 0; i < helpFileNames.length; i++) {
                tasks.push(_loadFile(helpUrl + helpFileNames[i], false)); //load doc files from manifest.json
              }
            } else {
              lib.log.verbose("cannot determine help documentation files from manifest.json");
            }

            lib.async.parallelLimit(tasks, 5, function (err, results) {
              if(err) {
                lib.log.error("Error: error occurred while loading files: ",e);
                process.exit(1);
              }
              if (callback)
                callback();
            });
          }
          break;
        case "md":
          module.exports.helpFiles[fileName] = body;
          if (callback)
            callback();
          break;
        default :
          if (callback)
            callback();
      }
    });
  };
}

module.exports.renderHelp = function(title, help, cmd, usage) {
  // add all of the base options
  addOpts({
    appOptsHelp:
      '  -h, --help                       print this usage message\n' +
      '  -e, --url url                    specify the C3 server to connect to\n' +
      '  -v, --verbose                    verbose output\n' +
      '  -q, --quiet                      no output except errors\n' +
      '  -x, --proxy                      proxy server address http://<user>:<password>@<server>.com:<port>\n' +
      '  -u, --user user                  username for authentication\n' +
      '  -p, --password password          password for authentication\n' +
      '  -k, --key filename               path to the file containing c3 key for authentication\n' +
      '  -T, --auth-token token           c3auth token to use for authentication\n' +
      '  -l, --locale locale              select the locale for data translation'
  });

  // if it's a tenant+tag specific command, add those options
  if(!opts['no-tenant-check'] && !opts.provKey && !opts.deleteKey && !opts.version) {
    addOpts({
      appOptsHelp: 
        '\n  -t, --tenant tenant              select the tenant to use\n' +
        '  -g, --tag tag                    select the tag to use'
    });
  }

  // if no usage string is specified, add a default
  usage = usage || '**Usage: ' + ((cmd && cmd.trim()) || 'cmd ') + ' [ options ] [ arguments ... ]**';
  var renderedTitle;
  var renderedHelp;
  var renderedUsage;
  try {
    renderedTitle = lib.md(title);
    renderedHelp = lib.md(help);
    renderedUsage = lib.md(usage);
  } catch (e) {
    lib.log.info(e);
    var error = "Rendering markdown encountered an error. Printing the raw markdown: ";
    lib.log.error(error);
    renderedTitle = title;
    renderedHelp = help;
    renderedUsage = usage;
  }

  addOpts({
    title: renderedTitle,
    appOptsHelp: renderedHelp,
    usage: renderedUsage
  });

  ifHelp();
}

/**
 * Calls on module.exports.main() in index.js in backend apps in /cli/, /prov/, /tester/ and executes the scripts.
 */
function start() {
  printVersion();

  if (typeof module.exports.help == 'function') {
    module.exports.help(opts);
  }

  var legalOpts = [
    // added in c3 via ADD_OPTS
    'version',
    'zip',
    'ls',
    'all-tags',
    'one-tag',
    'provData',
    'provUserCmd',
    'provKey',
    'deleteKey',
    'help',
    // these are all keys added in boot.js
    'program',
    'startTime',
    '_',
    '$0',
    'https',
    'hostname',
    'port',
    'protocol',
    'appOptsHelp',
    'isBasicAuth',
    'isC3Auth',
    'isC3KeyAuth',
    'appLocalPath',
    'keyFile',
    'userFile',
    'adminUser',
    'adminPass',
    'pvtKey',
    'authToken',
    'title',
    'usage',
    'no-tenant-check',
    'agent',
    'c', 'compat',
    // these are the options from boot.js
    'h', 'help',
    'e', 'url',
    'v', 'verbose',
    'q', 'quiet',
    'x', 'proxy',
    'u', 'user',
    'p', 'password',
    'k', 'key',
    'T', 'auth-token',
    'l', 'locale',
    't', 'tenant',
    'g', 'tag',
    'A', 'app',
  ];
  if (typeof module.exports.legalOpts === 'object') {
    legalOpts = legalOpts.concat(module.exports.legalOpts);
  }
  
  // lib.log.info('start,legalOpts: ' + legalOpts);
  // lib.log.info('start,opts\nkeys=' + Object.keys(opts) + '\nvalues=' + Object.values(opts));
  Object.keys(opts).forEach(function (key) {
    if (legalOpts.indexOf(key) < 0) {
      lib.log.fatal('unexpected input: \'-' + key + '\'.' + helpMsg);
    }
  });

  // lib.log.info('start,opts._: ' + opts._);
  // param 1 is the command like 'prov', 'tester', etc
  if (opts._.length > 1) {
    lib.log.fatal('unexpected input: \'' + opts._.slice(1, opts._.length) + '\'.' + helpMsg);
  }

  if ((!opts.tenant || !opts.tag) &&
      !opts.help &&
      !opts['no-tenant-check'] &&
      !opts.provKey &&
      !opts.deleteKey &&
      !opts.version) {
    lib.log.fatal("Error: " + opts.program + ': no ' + (!opts.tenant ? 'tenant' : 'tag') + ' specified.' + helpMsg);
  }

  if (typeof module.exports.main == 'function') {
    module.exports.main(opts);
  } else {
    lib.log.error("Error: " + opts.program + ': application did not define a main');
    process.exit(1);
  }
}

var getFileContents = function(file) {
  try {
    return lib.fs.readFileSync(file).toString('ascii');
  } catch (e){
    throw new Error("Error: Error while reading file " + file + ": " + e.message);
  }
};

/**
 * wrapper around getFileContents
 */
var getFileContentsIfPossible = function(file){
  try {
    return getFileContents(file);
  } catch (e){
    return null;
  }
};

/**
 * This function is called (by boot() and env_node.js) when you use c3 prov key instead of basic auth -u -p to log in.
 *
 * Sends over auth token in the format of BA:date:signature of date using private key. Date has to be in long milliseconds
 * (ie 1524870360035). If it's in date format (2018-04-25T17:59:45.104-07:00) you will not be able to authenticate via
 * c3 prov key. This is a known issue for c3 node
 *
 * Server verifies authentication in AuthenticatorMethods.java validateKeyAuthToken()
 */
var getC3KeyTokenGenerator = function () {
  var pvtKey = opts.pvtKey;
  var crypto = lib.crypto;
  var adminUser = opts.adminUser;
  var log = lib.log;

  return function() {
    if (!pvtKey) throw new Error("Cannot generate c3key auth token with private key");

    var signAlgo = 'RSA-SHA512';
    var signatureText = Date.now().toString();
    var signer = crypto.createSign(signAlgo);
    signer.update(signatureText);
    var signature = signer.sign(pvtKey, 'base64');
    var tokenString = adminUser + ":" + Buffer.from(signatureText).toString('base64') + ":" + signature;
    var authToken = "c3key " + Buffer.from(tokenString).toString('base64');

    log.verbose("Generated new token: " + authToken);

    return authToken;
  }
};

/**
 * Load JS type system located in c3server/love/all/src/main/resources/c3/love/typesys/js as well as backend script logic
 * located in /prov/, /cli/, /tester/, or /testerNode/ depending on which script you executed
 */
function loadApp() {
  var staticUrl = '/typesys/1/all.js?env=node',
    appUrl;

  if(opts.compat) { staticUrl += "&compat"; }  // for some reason this line is really crucial to c3 node. This is so screwed up

  //load the client
  var lf = _loadFile(staticUrl, true); // load js type system
  lf( function() {
    if (opts.adminUser) opts.user = opts.adminUser;

    if (/.+:.+/.test(opts.tenant)) {
      //Support for comma separated multi-tenant, multi-tag provisioning
      var tenantTags = opts.tenant.split(",");

      if (tenantTags.length == 1) {
        opts.tag = opts.tag || opts.tenant.replace(/^[^:]*:/, '');
        opts.tenant = opts.tenant.replace(/:.*/, '');
      }
    }

    C3.client.setup(opts); // this function is in env_node.js. Initializes C3.client in env_client.js

    //load the application
    if(opts.app){
      appUrl = '/static/nodejs-apps/' + opts.app + '/manifest.json';
      var lf = _loadFile(appUrl, false); // load script backend code
      lf(start);
    } else {
      start();
    }
  });
};

function getUsernameFile() {
  var result = {};
  result.writeFile = opts.keyFile + "." + opts.hostname + ".user";
  result.readFile = lib.fs.existsSync(result.writeFile) ? result.writeFile : opts.keyFile + ".user";
  return result;
}

function promptKey() {
  if(opts.help) return;

  lib.log.info(lib.chalk.black.bold("Creating your c3authkey"));
  var readlineSync = require('readline-sync');

  opts.adminUser = readlineSync.question(lib.chalk.green.bold(" What is your username? "));

  opts.adminPass = readlineSync.question(lib.chalk.green.bold(" What is your password? "), {
    hideEchoBack: true,
    mask: ''
  });

  opts.isBasicAuth = true;
  opts.authToken = 'Basic ' + Buffer.from(opts.adminUser + ':' + opts.adminPass).toString('base64');
}

function getPrivateKeyFile() {
  try {
    var keyDir = opts.keyFile.split('/');
    keyDir = keyDir.slice(0, keyDir.length-1).join('/');
    return keyDir;
  } catch (e) {
    return null;
  }
}

function deleteKey() {
  var keyDir = getPrivateKeyFile();

  if(keyDir) {
    if (lib.fs.existsSync(keyDir)) {
      lib.fs.readdirSync(keyDir).forEach(function(file, index) {
        lib.fs.unlinkSync(lib.path.join(keyDir, file));
      });
      lib.log.info("Deleted your c3authkey");
    }
  } else {
    lib.log.info("Failed to delete your c3authkey");
  }
}

/**
 * checks for authentication then runs loadApp()
 */
function boot(opts,callback) {
  if (!opts.app) lib.log.fatal("Error: " + opts.program + ': no application specified.' + helpMsg);

  opts.program += '-' + opts.app;
  opts.appLocalPath = "/usr/local/share/c3/nodeapps/" + opts.app;

  opts.keyFile = opts.keyFile || (process.env.HOME + "/.c3/c3-rsa");  // ~/.c3/c3-rsa
  opts.userFile = getUsernameFile();  // ~/.c3/c3-rsa.<server>.user

  if (isServerAccessible) {
    if(!opts.authToken) {
      if(!opts.adminUser) {
        var userFromFile = getFileContentsIfPossible(opts.userFile.readFile);
        if(userFromFile) {
          opts.adminUser = userFromFile.replace(/(\r\n|\n|\r)/gm,"");
        }
      }

      if (/.+:.+/.test(opts.adminUser)) {
        opts.adminPass = opts.adminPass || opts.adminUser.replace(/^[^:]*:/, '');
        opts.adminUser = opts.adminUser.replace(/:.*/, '');
      }

      if(opts.adminPass) {
        opts.isBasicAuth = true;
      }

      if(!opts.isBasicAuth) {
        opts.pvtKey = getFileContentsIfPossible(opts.keyFile);

        if(opts.deleteKey) {
          if(opts.pvtKey) {
            deleteKey();
          } else {
            lib.log.info(lib.chalk.bold("You have not created your c3authkey. Please create your c3authkey using 'c3 key'"));
          }
          process.exit(!opts.pvtKey ? 1 : 0);
        } else if(opts.pvtKey) {
          if(opts.provKey && !opts.help) {
            promptKey();
          } else {
            global.generateC3KeyAuthToken = getC3KeyTokenGenerator(); // store this function to be called in env_node.js
            opts.authToken = global.generateC3KeyAuthToken();
            opts.isC3KeyAuth = true;
          }
        } else {
          if(!opts.help) {
            if(!opts.provKey) {
              lib.log.error(lib.chalk.bold("You have not created your c3authkey. Please create your c3authkey using 'c3 key'"));
              process.exit(1);
            }
            promptKey();
          }
        }
      } else {
        opts.authToken = 'Basic ' + Buffer.from(opts.adminUser + ':' + opts.adminPass).toString('base64');
      }

      lib.log.verbose("Using username " + opts.adminUser + " for authentication");
    } else {
      opts.isC3Auth = true;
    }

    lib.log.verbose("Authentication token: " + opts.authToken);

    var params = {
      hostname: opts.hostname,
      port: opts.port,
      path: '/auth/1/login',
      headers: {
        Authorization: opts.authToken
      }
    };

    if (opts.agent) {
      lib.log.verbose("Agent added to http request params in boot.js")
      params.agent = opts.agent;
    }

    var proto = lib[opts.protocol];
    proto.get(params, function(res) {
      if (res.statusCode != 200) {
        var isBasicAuth = opts.isBasicAuth ? ' and password ******' : ' with c3Key file ' + opts.keyFile + ". You can provision your keys using 'c3 key'.";
        var isC3Auth = opts.isC3Auth ? 'with token ' + opts.authToken : 'with username ' + opts.adminUser + isBasicAuth;
        lib.log.error("Error: " + opts.program + ': unable to login ' + isC3Auth + ' (HTTP status: ' + res.statusCode + ')');
        lib.log.error("Failed to create your c3authkey.");

        if (!opts.help)
          process.exit(1);
        isServerAccessible = false;
        lib.log.info("Using cache. To get updated information, please connect to the server.");
      }
      loadApp();
    }).on('error', function(e) {
      lib.log.error("Error: " + opts.program + ': unable to connect to server ' + opts.url + ' (' + e.code + ')');
      if (!opts.help)
        process.exit(1);
      isServerAccessible = false;
      loadApp();
      lib.log.info("Using cache. To get updated information, please connect to the server.");
    });
  } else {
    lib.log.info("server not accessible");
    loadApp();
  }
}
//# sourceURL=boot.js