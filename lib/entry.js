'use strict';

var DIRECTORY_MODE = 16877;

module.exports = Entry;
function Entry(relativePath, size, mtime, checksum) {
  var isDirectory = relativePath.charAt(relativePath.length - 1) === '/';

   // ----------------------------------------------------------------------
   // required properties

  this.relativePath = relativePath;
  this.mode = isDirectory ? DIRECTORY_MODE : 0;
  this.size = size;
  this.mtime = mtime;
  this.checksum = checksum;
}

Entry.isDirectory = function (entry) {
  return (entry.mode & 61440) === 16384;
};

Entry.isFile = function (entry) {
  return !Entry.isDirectory(entry);
};

// required methods

Entry.prototype.isDirectory = function() {
  return Entry.isDirectory(this);
};
