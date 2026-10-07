// Minimal stand-ins for the NinjaTrader 8 API surface ZeroGexGoat.cs touches.
// Purpose: compile the real strategy file so syntax errors, typos, bad
// argument counts and unassigned locals surface here instead of in Jim's
// NinjaScript Editor. Signatures mirror NT8's documented ones.
//
// This proves the file is internally consistent and syntactically valid. It
// cannot prove the NT8 API matches these stubs.

using System;

namespace NinjaTrader.Core.FloatingPoint
{
    public static class DoubleExtensions
    {
        public static int ApproxCompare(this double a, double b)
        {
            return Math.Abs(a - b) < 1e-9 ? 0 : (a < b ? -1 : 1);
        }
    }
}

namespace NinjaTrader.Cbi
{
    public enum MarketPosition { Flat, Long, Short }
    public enum OrderFillResolution { Standard, High }
}

namespace NinjaTrader.Data
{
    public enum MaximumBarsLookBack { TwoHundredFiftySix, Infinite }
}

namespace NinjaTrader.Gui
{
    public class CategoryOrderAttribute : Attribute
    {
        public CategoryOrderAttribute(string name, int order) { }
    }
}

namespace NinjaTrader.NinjaScript
{
    using NinjaTrader.Cbi;
    using NinjaTrader.Data;

    public enum State { SetDefaults, Configure, DataLoaded, Historical, Realtime, Terminated }
    public enum Calculate { OnBarClose, OnEachTick, OnPriceChange }
    public enum EntryHandling { AllEntries, UniqueEntries }
    public enum StartBehavior { WaitUntilFlat, ImmediatelySubmit, AdoptAccountPosition }
    public enum TimeInForce { Day, Gtc }
    public enum RealtimeErrorHandling { StopCancelClose, IgnoreAllErrors, TakeNoAction }
    public enum StopTargetHandling { PerEntryExecution, ByStrategyPosition }
    public enum CalculationMode { Currency, Percent, Pips, Ticks, Price }

    public class NinjaScriptPropertyAttribute : Attribute { }

    public interface ISeries<T>
    {
        T this[int barsAgo] { get; }
    }

    public class Series<T> : ISeries<T>
    {
        private readonly System.Collections.Generic.Dictionary<int, T> values
            = new System.Collections.Generic.Dictionary<int, T>();

        public Series(object owner) { }
        public Series(object owner, MaximumBarsLookBack lookBack) { }

        public T this[int barsAgo]
        {
            get { T v; return values.TryGetValue(barsAgo, out v) ? v : default(T); }
            set { values[barsAgo] = value; }
        }
    }

    // Indicator return types. Each is an ISeries<double> so it can back the
    // midband / EE line fields and be indexed directly.
    public class IndicatorBase : ISeries<double>
    {
        public double this[int barsAgo] { get { return 0.0; } }
    }

    public class MIN : IndicatorBase { }
    public class MAX : IndicatorBase { }
    public class SMA : IndicatorBase { }
    public class EMA : IndicatorBase { }
    public class WMA : IndicatorBase { }
    public class HMA : IndicatorBase { }
    public class DEMA : IndicatorBase { }
    public class TEMA : IndicatorBase { }
    public class StdDev : IndicatorBase { }

    public class PositionInfo
    {
        public MarketPosition MarketPosition { get; set; }
        public double AveragePrice { get; set; }
    }

    public abstract class NinjaScriptBase
    {
        protected State State;

        public string Description { get; set; }
        public string Name { get; set; }
        public Calculate Calculate { get; set; }
        public MaximumBarsLookBack MaximumBarsLookBack { get; set; }
        public int BarsRequiredToTrade { get; set; }
        public int CurrentBar { get; protected set; }
        public double TickSize { get; protected set; }

        public ISeries<double> Open { get; protected set; }
        public ISeries<double> High { get; protected set; }
        public ISeries<double> Low { get; protected set; }
        public ISeries<double> Close { get; protected set; }

        protected virtual void OnStateChange() { }
        protected virtual void OnBarUpdate() { }

        protected MIN MIN(ISeries<double> input, int period) { return new MIN(); }
        protected MAX MAX(ISeries<double> input, int period) { return new MAX(); }
        protected SMA SMA(int period) { return new SMA(); }
        protected SMA SMA(ISeries<double> input, int period) { return new SMA(); }
        protected EMA EMA(int period) { return new EMA(); }
        protected WMA WMA(int period) { return new WMA(); }
        protected HMA HMA(int period) { return new HMA(); }
        protected DEMA DEMA(int period) { return new DEMA(); }
        protected TEMA TEMA(int period) { return new TEMA(); }
        protected StdDev StdDev(ISeries<double> input, int period) { return new StdDev(); }
    }
}

namespace NinjaTrader.NinjaScript.Indicators
{
    // Real NinjaScript puts indicators here; the strategy's `using` for it must
    // resolve even though every type it needs lives in the parent namespace.
    public class IndicatorMarker { }
}

namespace NinjaTrader.NinjaScript.Strategies
{
    using NinjaTrader.Cbi;

    public abstract class Strategy : NinjaScriptBase
    {
        public int EntriesPerDirection { get; set; }
        public EntryHandling EntryHandling { get; set; }
        public bool IsExitOnSessionCloseStrategy { get; set; }
        public int ExitOnSessionCloseSeconds { get; set; }
        public bool IsFillLimitOnTouch { get; set; }
        public OrderFillResolution OrderFillResolution { get; set; }
        public int Slippage { get; set; }
        public StartBehavior StartBehavior { get; set; }
        public TimeInForce TimeInForce { get; set; }
        public bool TraceOrders { get; set; }
        public RealtimeErrorHandling RealtimeErrorHandling { get; set; }
        public StopTargetHandling StopTargetHandling { get; set; }
        public bool IsInstantiatedOnEachOptimizationIteration { get; set; }

        public PositionInfo Position { get; protected set; }

        protected void EnterLong(int quantity, string signalName) { }
        protected void EnterShort(int quantity, string signalName) { }
        protected void ExitLong(int quantity, string signalName, string fromEntrySignal) { }
        protected void ExitShort(int quantity, string signalName, string fromEntrySignal) { }

        protected void SetStopLoss(string fromEntrySignal, CalculationMode mode, double value, bool isSimulatedStop) { }
        protected void SetProfitTarget(string fromEntrySignal, CalculationMode mode, double value) { }
        protected void SetTrailStop(string fromEntrySignal, CalculationMode mode, double value, bool isSimulatedStop) { }
    }
}

// System.ComponentModel.DataAnnotations is not in this mono install, and the
// real attributes are only metadata, so stand-ins with the same constructor
// shapes are enough to typecheck the strategy's usage.
namespace System.ComponentModel.DataAnnotations
{
    [AttributeUsage(AttributeTargets.All, AllowMultiple = false)]
    public class DisplayAttribute : Attribute
    {
        public string Name { get; set; }
        public int Order { get; set; }
        public string GroupName { get; set; }
        public string Description { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class RangeAttribute : Attribute
    {
        public RangeAttribute(int minimum, int maximum) { }
        public RangeAttribute(double minimum, double maximum) { }
        public RangeAttribute(Type type, string minimum, string maximum) { }
    }
}
