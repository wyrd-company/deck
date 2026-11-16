# Run task demo. The deck executable is the real CLI with an in-process fixture
# API. Its local SQLite store and read cursors are real; all data is synthetic.
Output docs/user/assets/demo.gif
Require deck
Set Shell "bash"
Set FontFamily "DejaVu Sans Mono"
Set FontSize 18
Set Width 1200
Set Height 700
Set Theme "Catppuccin Mocha"
Set Padding 28
Set TypingSpeed 22ms
Set Framerate 15
Set WindowBar Colorful
Set WindowBarSize 30
Hide
Type `export PS1='$ ' && clear` Enter
Show
Type `# deck / offline demo / synthetic conversation` Enter
Sleep 1s
Type `deck channels --session demo=release` Enter
Sleep 2s
Type `deck post --session demo=release --title 'Release timing' --text 'Can we ship today?'` Enter
Sleep 3s
Ctrl+L
Type `# A synthetic reviewer replies while we listen.` Enter
Type `deck listen --session demo=release --timeout 5m --keep-unread` Enter
Sleep 3s
Type `deck inbox --session demo=release` Enter
Sleep 2s
Ctrl+L
Type `deck read --session demo=release` Enter
Sleep 3s
Type `deck reply 1571484634081529857 --session demo=release --text 'Thanks, preparing it.'` Enter
Sleep 2s
Type `deck resolve 1571484634081529857` Enter
Sleep 3s
