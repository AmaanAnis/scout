//@version=6
indicator("BOS (1m)", overlay=true, max_lines_count=500, max_labels_count=500)

// ── Inputs ──
swingLen    = input.int(5, "Swing Length (bars each side)", minval=1)
maxLevels   = input.int(8, "Max Active Levels Per Side", minval=1, maxval=40)
minGapPct   = input.float(0.03, "Min Gap Between Levels (%)", minval=0.0, step=0.01)
useClose    = input.bool(true, "Confirm Break On Candle Close")
showChoch   = input.bool(true, "Label CHoCH (trend reversal breaks)")
resColor    = input.color(color.red, "Resistance (unbroken)")
supColor    = input.color(color.green, "Support (unbroken)")
bullColor   = input.color(color.green, "Bullish Break Color")
bearColor   = input.color(color.red, "Bearish Break Color")
lineWidth   = input.int(1, "Line Width", minval=1, maxval=4)
labelTransp = input.int(100, "Label Background Transparency", minval=0, maxval=100)

groupF = "Filters"
minBreakATR = input.float(0.2, "Min Break Size (x ATR)", minval=0.0, step=0.05, group=groupF)
maxDistATR  = input.float(8.0, "Hide Levels Farther Than (x ATR, 0 = off)", minval=0.0, step=0.5, group=groupF)
maxAgeBars  = input.int(500, "Hide Levels Older Than (bars, 0 = off)", minval=0, group=groupF)

groupH = "Higher-Timeframe Filter"
useHTF      = input.bool(true, "Use Higher-Timeframe Trend", group=groupH)
htf         = input.timeframe("15", "Higher Timeframe", group=groupH)
htfLen      = input.int(50, "HTF EMA Length", minval=2, group=groupH)
hideCounter = input.bool(false, "Hide Counter-Trend Signals (off = dim them)", group=groupH)

groupW = "Early Warning"
showWarn  = input.bool(true, "Show Early Warnings", group=groupW)
proxMult  = input.float(0.5, "Proximity (x ATR) to Nearest Level", minval=0.1, step=0.1, group=groupW)
minScore  = input.int(2, "Min Pressure Score (1-3)", minval=1, maxval=3, group=groupW)
useVolume = input.bool(true, "Use Volume Factor (turn off for forex)", group=groupW)
volMult   = input.float(1.3, "Volume Spike (x 20-bar avg)", minval=1.0, step=0.1, group=groupW)

// ── State ──
var array<line> resLines = array.new<line>()
var array<line> supLines = array.new<line>()
var int   trend        = 0   // 1 = bullish structure, -1 = bearish structure
var float lastWarnRes  = na
var float lastWarnSup  = na

// ── Core series ──
atr = ta.atr(14)
float brkSize = na(atr) ? 0.0 : minBreakATR * atr

// Higher-timeframe trend (non-repainting): 1 = up, -1 = down
int htfTrend = request.security(syminfo.tickerid, htf, close[1] > ta.ema(close, htfLen)[1] ? 1 : -1, lookahead=barmerge.lookahead_on)
bool counterBull = useHTF and htfTrend == -1
bool counterBear = useHTF and htfTrend == 1

// ── Helpers ──
tooClose(array<line> arr, float price) =>
    bool result = false
    if arr.size() > 0
        for i = 0 to arr.size() - 1
            lvl = arr.get(i).get_y1()
            if math.abs(lvl - price) / price * 100 < minGapPct
                result := true
                break
    result

isHidden(line ln, float atrVal) =>
    float lvl = ln.get_y1()
    bool tooOld = maxAgeBars > 0 and (bar_index - ln.get_x1()) > maxAgeBars
    bool tooFar = maxDistATR > 0 and not na(atrVal) and math.abs(close - lvl) > maxDistATR * atrVal
    tooOld or tooFar

// ── Swing detection -> new levels ──
ph = ta.pivothigh(high, swingLen, swingLen)
pl = ta.pivotlow(low, swingLen, swingLen)

if not na(ph) and not tooClose(resLines, ph)
    l = line.new(bar_index - swingLen, ph, bar_index, ph, color=resColor, width=lineWidth, extend=extend.none)
    resLines.push(l)
    if resLines.size() > maxLevels
        line.delete(resLines.shift())

if not na(pl) and not tooClose(supLines, pl)
    l = line.new(bar_index - swingLen, pl, bar_index, pl, color=supColor, width=lineWidth, extend=extend.none)
    supLines.push(l)
    if supLines.size() > maxLevels
        line.delete(supLines.shift())

// ── Break detection / line extension ──
float upSrc = useClose ? close : high
float dnSrc = useClose ? close : low

bool  bullBreak    = false
bool  bearBreak    = false
float bullBreakLvl = na
float bearBreakLvl = na
int   bullBreakX   = na
int   bearBreakX   = na

if resLines.size() > 0
    for i = resLines.size() - 1 to 0
        ln = resLines.get(i)
        lvl = ln.get_y1()
        ln.set_x2(bar_index)
        if upSrc > lvl + brkSize
            ln.set_color(bullColor)
            ln.set_style(line.style_dashed)
            ln.set_width(lineWidth)
            bullBreak := true
            if na(bullBreakLvl) or lvl > bullBreakLvl
                bullBreakLvl := lvl
                bullBreakX := ln.get_x1()
            resLines.remove(i)
        else
            ln.set_width(lineWidth)
            ln.set_color(isHidden(ln, atr) ? color.new(resColor, 100) : resColor)

if supLines.size() > 0
    for i = supLines.size() - 1 to 0
        ln = supLines.get(i)
        lvl = ln.get_y1()
        ln.set_x2(bar_index)
        if dnSrc < lvl - brkSize
            ln.set_color(bearColor)
            ln.set_style(line.style_dashed)
            ln.set_width(lineWidth)
            bearBreak := true
            if na(bearBreakLvl) or lvl < bearBreakLvl
                bearBreakLvl := lvl
                bearBreakX := ln.get_x1()
            supLines.remove(i)
        else
            ln.set_width(lineWidth)
            ln.set_color(isHidden(ln, atr) ? color.new(supColor, 100) : supColor)

// ── Label breaks as BOS or CHoCH (label sits on the level line, not on the candle) ──
bool bullBOS   = false
bool bearBOS   = false
bool bullChoch = false
bool bearChoch = false

if bullBreak
    bool isChoch = trend == -1
    if not (counterBull and hideCounter)
        if isChoch
            bullChoch := true
        else
            bullBOS := true
        if not isChoch or showChoch
            int lx = bar_index - math.min(math.round((bar_index - bullBreakX) / 2), 100)
            color tc = counterBull ? color.new(bullColor, 60) : bullColor
            label.new(lx, bullBreakLvl, isChoch ? "CHoCH" : "BOS", style=label.style_label_down, color=color.new(tc, labelTransp), textcolor=tc, size=size.tiny)
    trend := 1

if bearBreak
    bool isChoch = trend == 1
    if not (counterBear and hideCounter)
        if isChoch
            bearChoch := true
        else
            bearBOS := true
        if not isChoch or showChoch
            int lx = bar_index - math.min(math.round((bar_index - bearBreakX) / 2), 100)
            color tc = counterBear ? color.new(bearColor, 60) : bearColor
            label.new(lx, bearBreakLvl, isChoch ? "CHoCH" : "BOS", style=label.style_label_up, color=color.new(tc, labelTransp), textcolor=tc, size=size.tiny)
    trend := -1

// ── Find nearest visible active levels ──
line  nearRes    = na
float nearResLvl = na
if resLines.size() > 0
    for i = 0 to resLines.size() - 1
        ln = resLines.get(i)
        lvl = ln.get_y1()
        if not isHidden(ln, atr) and (na(nearResLvl) or lvl < nearResLvl)
            nearResLvl := lvl
            nearRes := ln

line  nearSup    = na
float nearSupLvl = na
if supLines.size() > 0
    for i = 0 to supLines.size() - 1
        ln = supLines.get(i)
        lvl = ln.get_y1()
        if not isHidden(ln, atr) and (na(nearSupLvl) or lvl > nearSupLvl)
            nearSupLvl := lvl
            nearSup := ln

// ── Early warning ──
avgVol   = ta.sma(volume, 20)
volSpike = useVolume and volume > avgVol * volMult

risingLows   = low > low[1] and low[1] > low[2]
bullCandles  = math.sum(close > open ? 1.0 : 0.0, 5) >= 3
bullScore    = (risingLows ? 1 : 0) + (bullCandles ? 1 : 0) + (volSpike ? 1 : 0)

fallingHighs = high < high[1] and high[1] < high[2]
bearCandles  = math.sum(close < open ? 1.0 : 0.0, 5) >= 3
bearScore    = (fallingHighs ? 1 : 0) + (bearCandles ? 1 : 0) + (volSpike ? 1 : 0)

nearHigh = not na(nearResLvl) and not na(atr) and (nearResLvl - close) <= proxMult * atr
nearLow  = not na(nearSupLvl) and not na(atr) and (close - nearSupLvl) <= proxMult * atr

bool warnBull = showWarn and nearHigh and bullScore >= minScore and not (counterBull and hideCounter)
bool warnBear = showWarn and nearLow and bearScore >= minScore and not (counterBear and hideCounter)

// Fire once per level (not every time conditions flicker on and off)
bool warnBullNew = warnBull and (na(lastWarnRes) or nearResLvl != lastWarnRes)
bool warnBearNew = warnBear and (na(lastWarnSup) or nearSupLvl != lastWarnSup)

if warnBullNew
    lastWarnRes := nearResLvl
if warnBearNew
    lastWarnSup := nearSupLvl

if warnBull and not na(nearRes)
    nearRes.set_width(lineWidth + 1)
if warnBear and not na(nearSup)
    nearSup.set_width(lineWidth + 1)

// Small markers instead of text labels
plotshape(warnBullNew, "Break Approaching (Up)", shape.diamond, location.belowbar, counterBull ? color.new(bullColor, 60) : bullColor, size=size.tiny)
plotshape(warnBearNew, "Break Approaching (Down)", shape.diamond, location.abovebar, counterBear ? color.new(bearColor, 60) : bearColor, size=size.tiny)

// ── Alerts ──
alertcondition(bullBOS, "Bullish BOS", "Bullish break of structure")
alertcondition(bearBOS, "Bearish BOS", "Bearish break of structure")
alertcondition(bullChoch, "Bullish CHoCH", "Bullish change of character")
alertcondition(bearChoch, "Bearish CHoCH", "Bearish change of character")
alertcondition(warnBullNew, "Bullish break approaching", "Price pressing against resistance")
alertcondition(warnBearNew, "Bearish break approaching", "Price pressing against support")
