#!/usr/bin/env node
// This file is for testing purposed only to compare signing
// algorithms between node.js and python.
//console.log( "Hello!" );
// lib here is lib from c3.js
var lib = {
crypto : require('crypto'),
fs : require('fs')
//chalk : require('chalk'),
//ora : require('ora'),
//md : require('ansimd')
};

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

var keyFile = process.env.HOME + "/.c3/c3-rsa"
//console.log(keyFile)
var pvtKey = getFileContentsIfPossible(keyFile);
//console.log(pvtKey)

//var pvtKey = '-----BEGIN RSA PRIVATE KEY-----\
//some random key with a bunch of stuff in it.\
//-----END RSA PRIVATE KEY-----\
//';
var crypto = lib.crypto;
var adminUser = "auser";
  
function getC3KeyToken() {
      if (!pvtKey) throw new Error("Cannot generate c3key auth token with private key");
  
      var signAlgo = 'RSA-SHA512';
      //var signatureText = Date.now().toString();
      var signatureText = '1626099164960'
      //console.log (signatureText);
      var signer = crypto.createSign(signAlgo);
      signer.update(signatureText);
      var signature = signer.sign(pvtKey, 'base64');
      console.log('signature::'+signature);
      var tokenString = adminUser + ":" + Buffer.from(signatureText).toString('base64') + ":" + signature;
      console.log('tokenString::'+tokenString);
      var authToken = "c3key " + Buffer.from(tokenString).toString('base64');
      //log.verbose("Generated new token: " + authToken);
      console.log('authToken::'+authToken);
      return authToken;
    };

getC3KeyToken();