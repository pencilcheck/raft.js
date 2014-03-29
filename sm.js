function Window(tabs) {
  this.tabs = tabs || [] // Ordered list, Left to right
}

function Tab(url) {
  this.url = url
}

module.exports = function () {
  this.windows = []
  this.activeWindowId = 0

  this.openTab = function (url, windowId) {
    var index = typeof windowId != 'undefined' ? windowId : this.activeWindowId
    if (!this.windows[index]) {
      this.openWindow()
      index = this.windows.length-1
    }
    this.windows[index].push(new Tab(url))
  }

  this.insertTab = function (url, windowId, index) {
    this.windows[windowId].splice(index, 1, new Tab(url))
  }

  this.modifyTab = function (url, tabId, windowId) {
    this.windows[windowId][tabId].url = url
  }

  this.closeTab = function (tabId, windowId) {
    this.windows[windowId].splice(tabId, 1)
  }

  this.openWindow = function () {
    this.windows.push(new Window())
  }

  this.closeWindow = function (windowId) {
    this.windows.splice(windowId, 1)
  }
}
