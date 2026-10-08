// ============================================================================
//  ZeroGexGoat — NinjaTrader 8 strategy
//
//  A codification of Jim Edwards' "GOAT" midband setup, written so every
//  judgment call in it is a PARAMETER rather than a decision baked in by the
//  person who typed it up. The point of this file is not to assert that the
//  setup works; it is to make the setup testable in Strategy Analyzer so the
//  question can be answered with a trade list instead of an opinion.
//
//  CONFIDENTIAL. This encodes a third party's trading method, shared in
//  confidence. It does not belong in a public directory or a published
//  package.
//
//  ---------------------------------------------------------------------
//  THE CHART THIS WAS BUILT FROM
//  ---------------------------------------------------------------------
//    Instrument : YM 12-26, 3 Range   (also run on NQ 12-26, 5 Range)
//    Indicators : Bollinger(YM 12-26 (3 Range), 2, 55)
//                 HMA(YM 12-26 (3 Range), 22)
//                 jeStochastics(..., 40, 3, 3, 3, K_Line, 60)
//    Calculate  : On bar close
//    Displacement: 0
//
//  The thick magenta line is the Bollinger MIDBAND (a 55-period average).
//  The thin yellow line is the HMA(22) — what Jim calls the EE (Entry/Exit)
//  line. The blue lines are the +/- 2 sigma bands. The "pinch" is the EE line
//  coming very close to the midband.
//
//  ---------------------------------------------------------------------
//  BAR COLORS — the part that is easy to get backwards
//  ---------------------------------------------------------------------
//  The entry rule is written in terms of bar COLOR, and those colors come
//  from jeStochastics' paint bars. Jim has REMAPPED the indicator's default
//  brushes, so the colors do NOT mean what the stock indicator's names
//  suggest. Read from his own settings dialog:
//
//    K > 60   and Close >  Open  ->  WHITE         (#FFFFFF)
//    K > 60   and Close <= Open  ->  DARK RED      (#8B0000)
//    K < 40   and Close >  Open  ->  FOREST GREEN  (#228B22)
//    K < 40   and Close <= Open  ->  BLACK         (#000000)
//    40<=K<=60 and Close >  Open ->  LIME          (#00FF00)
//    40<=K<=60 and Close <= Open ->  RED           (#FF0000)
//
//  So "green" and "white" are both up-closing bars at increasing K, and
//  "red" and "black" are both down-closing bars at decreasing K. Jim's
//  "sometimes price jumps from black to white and skips green, which is even
//  stronger" is a single bar carrying K from below 40 to above 60.
//
//  ---------------------------------------------------------------------
//  THE RULES, AS IMPLEMENTED
//  ---------------------------------------------------------------------
//  LONG  (mirror for short):
//    1. Midband slope is UP by at least MidSlopeMinTicks over
//       MidSlopeLookback bars.
//    2. The EE line has PINCHED the midband within the last
//       PinchLookbackBars: |EE - mid| <= PinchMaxTicks.
//    3. Price has not broken the midband against the trade since that pinch
//       (see RequireNoMidlineBreak / MidlineBreakBypassSlopeTicks — Jim asked
//       for flexibility here when slope is strong).
//    4. The entry bar closes UP and the bar before it was BLACK.
//    5. The entry bar closes AT OR ABOVE the EE line (within
//       EntryEeToleranceTicks). A bar that closes away from the EE line is
//       what Jim calls a REACHER, and a reacher is no trade.
//
//  EXITS — two lots:
//    Lot 1 ("GOAT1") exits on the first opposite-closing bar.
//    Lot 2 ("GOAT2") trails by TrailTicks.
//    Both carry an initial stop of the entry bar's range + StopExtraTicks,
//    which on a 3-range chart is a 4-tick stop.
//    When the opposite-closing bar leaves price within StopRaceGuardTicks of
//    lot 1's stop, lot 1 is left to that stop instead of being sent a
//    separate market exit. Live, the two filling together traded one
//    contract too many (see ExitTarget1).
//
//  Jim's second target is also allowed to be a PREDRAWN LEVEL (opening range,
//  ZeroGEX, Camarilla). That branch is deliberately NOT implemented here: a
//  backtest that fetched levels over HTTP per bar would be unusable, and the
//  level question is answered better by joining this strategy's exported
//  trade list to ZeroGEX's stored level history afterward.
//
//  ---------------------------------------------------------------------
//  READ THIS BEFORE TRUSTING A BACKTEST
//  ---------------------------------------------------------------------
//  Range bars are built from TICKS. If Strategy Analyzer does not have tick
//  data for the period, NinjaTrader will construct the range bars from
//  minute data and they will NOT match the bars on your live chart — same
//  settings, different bars, different trades, no error message. See
//  README.md in this folder for how to check that before reading any result.
// ============================================================================

#region Using declarations
using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.Gui;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.Indicators;
using NinjaTrader.Core.FloatingPoint;
#endregion

namespace NinjaTrader.NinjaScript.Strategies
{
	/// <summary>Moving-average family for the midband and the EE line.</summary>
	public enum GoatMaType
	{
		SMA,
		EMA,
		WMA,
		HMA,
		DEMA,
		TEMA,
	}

	/// <summary>The six paint-bar colors, as Jim's chart maps them.</summary>
	public enum GoatBarColor
	{
		White,
		DarkRed,
		ForestGreen,
		Black,
		Lime,
		Red,
	}

	/// <summary>What closes the first lot.</summary>
	public enum GoatTarget1Mode
	{
		FirstOppositeCloseBar,
		FixedTicks,
	}

	/// <summary>How the initial stop is sized.</summary>
	public enum GoatStopMode
	{
		EntryBarRangePlusTicks,
		FixedTicks,
	}

	public class ZeroGexGoat : Strategy
	{
		// -- indicator state ------------------------------------------------
		private Series<double> fastK;   // raw stochastic, before smoothing
		private SMA kSeries;            // jeStochastics' "slow %K"
		private MIN minLow;
		private MAX maxHigh;
		private StdDev stdDev;

		// Midband and EE line. ISeries<double> so any MA family can back them.
		private ISeries<double> mid;
		private ISeries<double> ee;

		// -- setup state ----------------------------------------------------
		//: Bar index of the most recent qualifying pinch, or -1 when the EE
		//: line has not been near the midband inside the lookback.
		private int lastPinchBar = -1;

		// -- trade state ----------------------------------------------------
		//: Lot 1 exits once, on the first opposite-closing bar. Without this
		//: the aggregate position is still Long after it leaves, so the exit
		//: would re-fire on every subsequent bar against a signal that no
		//: longer has a position.
		private bool target1Done;
		//: Stop distance in ticks, measured on the SIGNAL bar. Held because by
		//: the time the trail runs, the entry has filled and bar 0 is the bar
		//: AFTER the one the stop was sized from. On a range chart every bar
		//: has the same range so recomputing happened to agree, but on any
		//: time-based bar type it would have silently sized lot 2's stop off
		//: the wrong bar.
		private int signalStopTicks;
		//: Initial stop price, held so the lot 2 trail can never widen it.
		private double initialStopPrice;
		//: Best price reached since entry, which the lot 2 trail hangs off.
		private double runExtreme;
		//: Direction of the trade last entered. A position on the other side
		//: can only come from an exit that overfilled, never from an entry.
		private MarketPosition tradeDirection = MarketPosition.Flat;

		//: How close price may be to lot 1's stop before ExitTarget1 leaves the
		//: exit to that stop. Order plumbing, not a trading judgment, so it is
		//: not a parameter.
		private const int StopRaceGuardTicks = 2;

		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description = "Jim Edwards' GOAT midband setup, parameterised for Strategy Analyzer.";
				Name = "ZeroGexGoat";

				// Jim's chart runs "On bar close", and so does this. It is also
				// the only honest setting for a backtest: OnEachTick would let
				// the strategy see an intrabar high the historical data cannot
				// actually reconstruct.
				Calculate = Calculate.OnBarClose;
				EntriesPerDirection = 2;
				EntryHandling = EntryHandling.AllEntries;
				IsExitOnSessionCloseStrategy = true;
				ExitOnSessionCloseSeconds = 30;
				IsFillLimitOnTouch = false;
				MaximumBarsLookBack = MaximumBarsLookBack.TwoHundredFiftySix;
				OrderFillResolution = OrderFillResolution.Standard;
				Slippage = 0;
				StartBehavior = StartBehavior.WaitUntilFlat;
				TimeInForce = TimeInForce.Gtc;
				TraceOrders = false;
				RealtimeErrorHandling = RealtimeErrorHandling.StopCancelClose;
				StopTargetHandling = StopTargetHandling.PerEntryExecution;
				BarsRequiredToTrade = 100;
				IsInstantiatedOnEachOptimizationIteration = false;

				// --- midband (the Bollinger basis) ---
				MidMaType = GoatMaType.SMA;
				MidPeriod = 55;
				BandStdDev = 2.0;

				// --- EE line ---
				EeMaType = GoatMaType.HMA;
				EePeriod = 22;

				// --- stochastic / paint bars ---
				PeriodK = 3;
				SmoothK = 3;
				StochOverbought = 60;
				StochOversold = 40;

				// --- setup filters ---
				MidSlopeLookback = 10;
				MidSlopeMinTicks = 2;
				PinchMaxTicks = 4;
				PinchLookbackBars = 20;
				RequireNoMidlineBreak = true;
				MidlineBreakToleranceTicks = 0;
				MidlineBreakBypassSlopeTicks = 0;   // 0 == bypass disabled
				RequireColorFlip = true;
				RequireSkip = false;
				EntryEeToleranceTicks = 0;
				MinBandWidthTicks = 0;

				// --- sizing and exits ---
				LotSize = 1;
				StopMode = GoatStopMode.EntryBarRangePlusTicks;
				StopExtraTicks = 1;
				StopFixedTicks = 8;
				Target1Mode = GoatTarget1Mode.FirstOppositeCloseBar;
				Target1Ticks = 8;
				TrailTicks = 8;
			}
			else if (State == State.Configure)
			{
				// Set here, not in DataLoaded: NinjaTrader has already read
				// BarsRequiredToTrade by the time data loads, so assigning it
				// there is too late to have any effect.
				BarsRequiredToTrade = Math.Max(
					BarsRequiredToTrade,
					Math.Max(MidPeriod, EePeriod) + PinchLookbackBars + 5);
			}
			else if (State == State.DataLoaded)
			{
				fastK = new Series<double>(this);
				minLow = MIN(Low, PeriodK);
				maxHigh = MAX(High, PeriodK);
				kSeries = SMA(fastK, SmoothK);
				stdDev = StdDev(Close, MidPeriod);

				mid = MovingAverage(MidMaType, MidPeriod);
				ee = MovingAverage(EeMaType, EePeriod);
			}
		}

		/// <summary>Resolve an MA family + period to its series.</summary>
		private ISeries<double> MovingAverage(GoatMaType type, int period)
		{
			switch (type)
			{
				case GoatMaType.EMA:  return EMA(period);
				case GoatMaType.WMA:  return WMA(period);
				case GoatMaType.HMA:  return HMA(period);
				case GoatMaType.DEMA: return DEMA(period);
				case GoatMaType.TEMA: return TEMA(period);
				default:              return SMA(period);
			}
		}

		protected override void OnBarUpdate()
		{
			// fastK has to be written before kSeries is read: SMA(fastK, n)
			// pulls the value this bar just produced.
			UpdateFastK();

			if (CurrentBar < BarsRequiredToTrade)
				return;

			UpdatePinch();
			ManageOpenPosition();

			if (Position.MarketPosition == MarketPosition.Flat)
				LookForEntry();
		}

		// --------------------------------------------------------------
		// jeStochastics, reproduced
		// --------------------------------------------------------------
		//
		// Deliberately reimplemented rather than referenced, for two reasons:
		// a backtest should not depend on a third-party indicator being
		// installed, and jeStochastics' intrabar path (running max/min updated
		// per tick) differs from its on-bar-close path. Under
		// Calculate.OnBarClose only the branch below is ever taken, so this is
		// the same arithmetic the chart shows.
		private void UpdateFastK()
		{
			double hi = maxHigh[0];
			double lo = minLow[0];
			double den = hi - lo;

			if (den.ApproxCompare(0) == 0)
				fastK[0] = CurrentBar == 0 ? 50.0 : fastK[1];
			else
				fastK[0] = Math.Min(100.0, Math.Max(0.0, 100.0 * (Close[0] - lo) / den));
		}

		/// <summary>jeStochastics' "slow %K" — the line the colors key on.</summary>
		private double K(int barsAgo)
		{
			return kSeries[barsAgo];
		}

		/// <summary>The paint-bar color of a bar, per Jim's brush mapping.</summary>
		private GoatBarColor ColorOf(int barsAgo)
		{
			double k = K(barsAgo);
			bool up = Close[barsAgo] > Open[barsAgo];

			if (k > StochOverbought)
				return up ? GoatBarColor.White : GoatBarColor.DarkRed;
			if (k < StochOversold)
				return up ? GoatBarColor.ForestGreen : GoatBarColor.Black;
			return up ? GoatBarColor.Lime : GoatBarColor.Red;
		}

		// --------------------------------------------------------------
		// The pinch
		// --------------------------------------------------------------
		private void UpdatePinch()
		{
			if (Math.Abs(ee[0] - mid[0]) <= PinchMaxTicks * TickSize)
				lastPinchBar = CurrentBar;
			else if (lastPinchBar >= 0 && CurrentBar - lastPinchBar > PinchLookbackBars)
				lastPinchBar = -1;   // the pinch has gone stale
		}

		private bool HasRecentPinch()
		{
			return lastPinchBar >= 0 && CurrentBar - lastPinchBar <= PinchLookbackBars;
		}

		// --------------------------------------------------------------
		// Entry
		// --------------------------------------------------------------
		private void LookForEntry()
		{
			if (!HasRecentPinch())
				return;

			double slope = mid[0] - mid[MidSlopeLookback];
			double slopeThreshold = MidSlopeMinTicks * TickSize;

			// Jim: "Trouble comes with a flatter slope and narrow Bollinger
			// bands." The slope half is MidSlopeMinTicks; this is the other
			// half. 0 disables it.
			if (MinBandWidthTicks > 0
				&& 2.0 * BandStdDev * stdDev[0] < MinBandWidthTicks * TickSize)
				return;

			if (slope >= slopeThreshold && QualifiesLong(slope))
			{
				SetStops();
				tradeDirection = MarketPosition.Long;
				EnterLong(LotSize, "GOAT1");
				EnterLong(LotSize, "GOAT2");
			}
			else if (slope <= -slopeThreshold && QualifiesShort(slope))
			{
				SetStops();
				tradeDirection = MarketPosition.Short;
				EnterShort(LotSize, "GOAT1");
				EnterShort(LotSize, "GOAT2");
			}
		}

		private bool QualifiesLong(double slope)
		{
			// Rule 4 — the entry bar closes up, after a black bar.
			if (Close[0] <= Open[0])
				return false;
			if (RequireColorFlip && ColorOf(1) != GoatBarColor.Black)
				return false;
			// "Skipped green": one bar carried K from below oversold to above
			// overbought. Jim calls this the stronger version.
			if (RequireSkip && ColorOf(0) != GoatBarColor.White)
				return false;

			// Rule 5 — snuggler, not reacher.
			if (Close[0] < ee[0] - EntryEeToleranceTicks * TickSize)
				return false;

			return !MidlineBrokenAgainst(true, slope);
		}

		private bool QualifiesShort(double slope)
		{
			if (Close[0] > Open[0])
				return false;
			if (RequireColorFlip && ColorOf(1) != GoatBarColor.White)
				return false;
			if (RequireSkip && ColorOf(0) != GoatBarColor.Black)
				return false;

			if (Close[0] > ee[0] + EntryEeToleranceTicks * TickSize)
				return false;

			return !MidlineBrokenAgainst(false, slope);
		}

		/// <summary>
		/// True when price has broken the midband against the trade since the
		/// pinch. Jim's rule is "price can not go above the mid line" for a
		/// short; he then asked for flexibility on it when slope is strong,
		/// which is what MidlineBreakBypassSlopeTicks buys.
		/// </summary>
		private bool MidlineBrokenAgainst(bool isLong, double slope)
		{
			if (!RequireNoMidlineBreak)
				return false;

			if (MidlineBreakBypassSlopeTicks > 0
				&& Math.Abs(slope) >= MidlineBreakBypassSlopeTicks * TickSize)
				return false;

			double tolerance = MidlineBreakToleranceTicks * TickSize;
			int from = Math.Min(CurrentBar - lastPinchBar, PinchLookbackBars);

			for (int i = 0; i <= from; i++)
			{
				if (isLong && Close[i] < mid[i] - tolerance)
					return true;
				if (!isLong && Close[i] > mid[i] + tolerance)
					return true;
			}
			return false;
		}

		// --------------------------------------------------------------
		// Exits
		// --------------------------------------------------------------
		/// <summary>
		/// Arm the initial stop for both lots, sized off the entry bar.
		///
		/// Lot 2 is NOT given a SetTrailStop here. NinjaTrader will not honour
		/// SetStopLoss and SetTrailStop on the same entry signal — the later
		/// call simply replaces the earlier one — so arming both would have
		/// thrown away Jim's entry-bar stop and left lot 2 risking the full
		/// trail distance from the moment of entry. On a 3-range chart that is
		/// 8 ticks of risk where the rule says 4. The trail is applied by hand
		/// in <see cref="TrailLot2"/> instead, floored at this initial stop.
		/// </summary>
		private void SetStops()
		{
			int stopTicks = StopMode == GoatStopMode.FixedTicks
				? StopFixedTicks
				: (int)Math.Round((High[0] - Low[0]) / TickSize) + StopExtraTicks;

			// A degenerate range bar would otherwise ask for a zero-tick stop.
			stopTicks = Math.Max(1, stopTicks);
			signalStopTicks = stopTicks;

			SetStopLoss("GOAT1", CalculationMode.Ticks, stopTicks, false);
			SetStopLoss("GOAT2", CalculationMode.Ticks, stopTicks, false);

			if (Target1Mode == GoatTarget1Mode.FixedTicks)
				SetProfitTarget("GOAT1", CalculationMode.Ticks, Target1Ticks);

			// Entry fills on the next bar's open, so the stop price is not
			// known yet; AnchorStops sets it on the first bar in position.
			initialStopPrice = double.NaN;
			runExtreme = double.NaN;
			target1Done = false;
		}

		private void ManageOpenPosition()
		{
			if (Position.MarketPosition == MarketPosition.Flat)
			{
				target1Done = false;
				return;
			}

			AnchorStops();
			ExitTarget1();
			TrailLot2();
		}

		/// <summary>
		/// Fix the initial stop price and the trail's starting extreme on the
		/// first bar in position. Runs before ExitTarget1, which needs the stop
		/// price on that very bar.
		/// </summary>
		private void AnchorStops()
		{
			if (!double.IsNaN(initialStopPrice))
				return;

			bool isLong = Position.MarketPosition == MarketPosition.Long;
			double entry = Position.AveragePrice;

			initialStopPrice = isLong
				? entry - signalStopTicks * TickSize
				: entry + signalStopTicks * TickSize;
			runExtreme = isLong ? High[0] : Low[0];
		}

		/// <summary>
		/// "First target is the first green bar" — for a short that is the
		/// first up-closing bar; for a long, the mirror. Fires once: after the
		/// lot is gone the aggregate position is still open, so without the
		/// latch this would re-issue the exit on every later bar against an
		/// entry signal that no longer holds anything.
		///
		/// Not when price is already at lot 1's stop. Under OnBarClose a range
		/// bar is only seen to close when the next tick breaks out of it, and
		/// when the first bar after entry runs straight against the trade that
		/// tick IS the stop price. Live, the stop and a market exit then both
		/// filled and sold one contract too many, leaving an unprotected
		/// position on the wrong side (Jim's first live session, 2026-10-08). A
		/// backtest never shows it, because historical fills are processed
		/// before OnBarUpdate. So inside StopRaceGuardTicks lot 1 is left to its
		/// stop, at most that many ticks from where the market exit would have
		/// filled, and the latch stays open in case price turns back.
		/// </summary>
		private void ExitTarget1()
		{
			if (target1Done || Target1Mode != GoatTarget1Mode.FirstOppositeCloseBar)
				return;

			double guard = StopRaceGuardTicks * TickSize;

			if (Position.MarketPosition == MarketPosition.Long && Close[0] <= Open[0])
			{
				if (ExitSidePrice(true) - initialStopPrice <= guard)
					return;
				ExitLong(LotSize, "X1", "GOAT1");
				target1Done = true;
			}
			else if (Position.MarketPosition == MarketPosition.Short && Close[0] > Open[0])
			{
				if (initialStopPrice - ExitSidePrice(false) <= guard)
					return;
				ExitShort(LotSize, "X1", "GOAT1");
				target1Done = true;
			}
		}

		/// <summary>
		/// The worse of the bar's close and the live quote on the side lot 1
		/// exits into. Historically NinjaTrader substitutes the close for the
		/// quote; the close is also the fallback when a feed sends no quote, since
		/// a zero bid would otherwise read as "at the stop" on every bar.
		/// </summary>
		private double ExitSidePrice(bool isLong)
		{
			double quote = isLong ? GetCurrentBid() : GetCurrentAsk();
			if (quote <= 0)
				return Close[0];
			return isLong ? Math.Min(Close[0], quote) : Math.Max(Close[0], quote);
		}

		/// <summary>
		/// Trail lot 2 by TrailTicks off the best price seen since entry,
		/// never looser than the initial entry-bar stop.
		/// </summary>
		private void TrailLot2()
		{
			bool isLong = Position.MarketPosition == MarketPosition.Long;

			runExtreme = isLong
				? Math.Max(runExtreme, High[0])
				: Math.Min(runExtreme, Low[0]);

			double trail = isLong
				? runExtreme - TrailTicks * TickSize
				: runExtreme + TrailTicks * TickSize;

			double stop = isLong
				? Math.Max(initialStopPrice, trail)
				: Math.Min(initialStopPrice, trail);

			SetStopLoss("GOAT2", CalculationMode.Price, stop, false);
		}

		/// <summary>
		/// Backstop for the race ExitTarget1 guards against. Entries only happen
		/// when flat and always in tradeDirection, so a position on the other
		/// side can only be an exit that filled after the stops had already
		/// closed the trade. Close it at once rather than leave it unprotected:
		/// no stop is attached to it, and nothing else in this strategy would
		/// ever exit it before the session close.
		/// </summary>
		protected override void OnPositionUpdate(Cbi.Position position, double averagePrice,
			int quantity, Cbi.MarketPosition marketPosition)
		{
			if (State != State.Realtime
				|| marketPosition == MarketPosition.Flat
				|| tradeDirection == MarketPosition.Flat
				|| marketPosition == tradeDirection)
				return;

			Log(string.Format(
				"ZeroGexGoat: an exit overfilled and left {0} {1} against a {2} trade. Closing it.",
				quantity, marketPosition, tradeDirection), LogLevel.Warning);

			if (marketPosition == MarketPosition.Short)
				ExitShort("Overfill");
			else
				ExitLong("Overfill");
		}

		#region Properties

		[NinjaScriptProperty]
		[Display(Name = "Midband MA type", Order = 1, GroupName = "1. Midband")]
		public GoatMaType MidMaType { get; set; }

		[NinjaScriptProperty]
		[Range(2, int.MaxValue)]
		[Display(Name = "Midband period", Order = 2, GroupName = "1. Midband")]
		public int MidPeriod { get; set; }

		[NinjaScriptProperty]
		[Range(0.1, double.MaxValue)]
		[Display(Name = "Band std dev", Order = 3, GroupName = "1. Midband")]
		public double BandStdDev { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "EE line MA type", Order = 1, GroupName = "2. EE line")]
		public GoatMaType EeMaType { get; set; }

		[NinjaScriptProperty]
		[Range(2, int.MaxValue)]
		[Display(Name = "EE line period", Order = 2, GroupName = "2. EE line")]
		public int EePeriod { get; set; }

		[NinjaScriptProperty]
		[Range(1, int.MaxValue)]
		[Display(Name = "Stochastic %K period", Order = 1, GroupName = "3. Stochastic")]
		public int PeriodK { get; set; }

		[NinjaScriptProperty]
		[Range(1, int.MaxValue)]
		[Display(Name = "Stochastic %K smoothing", Order = 2, GroupName = "3. Stochastic")]
		public int SmoothK { get; set; }

		[NinjaScriptProperty]
		[Range(1.0, 100.0)]
		[Display(Name = "Overbought (green/white above)", Order = 3, GroupName = "3. Stochastic")]
		public double StochOverbought { get; set; }

		[NinjaScriptProperty]
		[Range(0.0, 99.0)]
		[Display(Name = "Oversold (red/black below)", Order = 4, GroupName = "3. Stochastic")]
		public double StochOversold { get; set; }

		[NinjaScriptProperty]
		[Range(1, int.MaxValue)]
		[Display(Name = "Slope lookback (bars)", Order = 1, GroupName = "4. Setup filters")]
		public int MidSlopeLookback { get; set; }

		[NinjaScriptProperty]
		[Range(0, int.MaxValue)]
		[Display(Name = "Min slope over lookback (ticks)", Order = 2, GroupName = "4. Setup filters")]
		public int MidSlopeMinTicks { get; set; }

		[NinjaScriptProperty]
		[Range(0, int.MaxValue)]
		[Display(Name = "Pinch: max EE-to-mid (ticks)", Order = 3, GroupName = "4. Setup filters")]
		public int PinchMaxTicks { get; set; }

		[NinjaScriptProperty]
		[Range(1, int.MaxValue)]
		[Display(Name = "Pinch: valid for (bars)", Order = 4, GroupName = "4. Setup filters")]
		public int PinchLookbackBars { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "Require no midline break", Order = 5, GroupName = "4. Setup filters")]
		public bool RequireNoMidlineBreak { get; set; }

		[NinjaScriptProperty]
		[Range(0, int.MaxValue)]
		[Display(Name = "Midline break tolerance (ticks)", Order = 6, GroupName = "4. Setup filters")]
		public int MidlineBreakToleranceTicks { get; set; }

		[NinjaScriptProperty]
		[Range(0, int.MaxValue)]
		[Display(Name = "Bypass break rule above slope (ticks, 0=off)", Order = 7, GroupName = "4. Setup filters")]
		public int MidlineBreakBypassSlopeTicks { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "Require color flip (black->up / white->down)", Order = 8, GroupName = "4. Setup filters")]
		public bool RequireColorFlip { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "Require skipped green/red (stronger)", Order = 9, GroupName = "4. Setup filters")]
		public bool RequireSkip { get; set; }

		[NinjaScriptProperty]
		[Range(0, int.MaxValue)]
		[Display(Name = "Entry close vs EE tolerance (ticks)", Order = 10, GroupName = "4. Setup filters")]
		public int EntryEeToleranceTicks { get; set; }

		[NinjaScriptProperty]
		[Range(0, int.MaxValue)]
		[Display(Name = "Min band width (ticks, 0=off)", Order = 11, GroupName = "4. Setup filters")]
		public int MinBandWidthTicks { get; set; }

		[NinjaScriptProperty]
		[Range(1, int.MaxValue)]
		[Display(Name = "Contracts per lot", Order = 1, GroupName = "5. Sizing and exits")]
		public int LotSize { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "Stop mode", Order = 2, GroupName = "5. Sizing and exits")]
		public GoatStopMode StopMode { get; set; }

		[NinjaScriptProperty]
		[Range(0, int.MaxValue)]
		[Display(Name = "Stop: extra ticks beyond entry bar", Order = 3, GroupName = "5. Sizing and exits")]
		public int StopExtraTicks { get; set; }

		[NinjaScriptProperty]
		[Range(1, int.MaxValue)]
		[Display(Name = "Stop: fixed ticks", Order = 4, GroupName = "5. Sizing and exits")]
		public int StopFixedTicks { get; set; }

		[NinjaScriptProperty]
		[Display(Name = "Target 1 mode", Order = 5, GroupName = "5. Sizing and exits")]
		public GoatTarget1Mode Target1Mode { get; set; }

		[NinjaScriptProperty]
		[Range(1, int.MaxValue)]
		[Display(Name = "Target 1: fixed ticks", Order = 6, GroupName = "5. Sizing and exits")]
		public int Target1Ticks { get; set; }

		[NinjaScriptProperty]
		[Range(1, int.MaxValue)]
		[Display(Name = "Lot 2 trail (ticks)", Order = 7, GroupName = "5. Sizing and exits")]
		public int TrailTicks { get; set; }

		#endregion
	}
}
