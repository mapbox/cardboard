var fs = require('fs');
var path = require('path');
var documentation = require('documentation');
var lint = require('documentation/streams/lint');
var github = require('documentation/streams/github');
var hierarchy = require('documentation/streams/hierarchy');
var md = require('documentation/streams/output/markdown');
var pkg = require('../package.json');

documentation(path.resolve(__dirname, '..', pkg.main))
  // .pipe(lint())
  .pipe(github())
  .pipe(hierarchy())
  .pipe(md({
    name: pkg.name,
    version: pkg.version,
    path: __dirname,
    template: path.resolve(__dirname, 'template.hbs')
  }))
  .on('data', function(file) {
    fs.writeFile(path.resolve(__dirname, '..', 'api.md'), file);
  });
