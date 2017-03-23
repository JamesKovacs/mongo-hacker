function time(func) {
  var start = new Date();
  func().shellPrint();
  var end = new Date();
  var timing = colorize("Completed in " + (end - start) + "ms", { color: 'green', bright: true });
  print(timing);
}
