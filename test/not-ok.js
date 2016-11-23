module.exports = function notOk(item, msg) {
  if (item === undefined) return;
  if (item != false && item !== null) throw new Error('failed fasly check' + (msg ? ' | '+msg : ''));
};

