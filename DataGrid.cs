/* 
 * Written by Ronnie Overby
 * and part of the Ronnie Overby Grab Bag: https://github.com/ronnieoverby/RonnieOverbyGrabBag
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows.Controls;
using System.Windows.Input;

namespace Overby.WPF.Controls
{
    public class DataGrid : System.Windows.Controls.DataGrid
    {
        public DataGrid()
        {
            SortDescriptions = new List<SortDescription>();
            Sorting += DataGridSorting;
        }

        protected List<SortDescription> SortDescriptions { get; private set; }

        public event EventHandler<SortConstructedEventArgs> SortConstructed;

        public void OnSortConstructed(SortConstructedEventArgs e)
        {
            var handler = SortConstructed;
            if (handler != null) handler(this, e);
        }

        private void DataGridSorting(object sender, DataGridSortingEventArgs e)
        {
            e.Handled = true;
            e.Column.SortDirection = e.Column.SortDirection != ListSortDirection.Ascending
                                         ? ListSortDirection.Ascending
                                         : ListSortDirection.Descending;

            var sd = new SortDescription(e.Column.SortMemberPath, e.Column.SortDirection.Value);

            if (ShiftPressed)
                SortDescriptions =
                    SortDescriptions.Where(x => x.PropertyName != sd.PropertyName).ToList();
            else
                SortDescriptions.Clear();

            SortDescriptions.Add(sd);

            OnSortConstructed(new SortConstructedEventArgs(SortDescriptions));
        }

        private bool ShiftPressed
        {
            get { return (Keyboard.Modifiers & ModifierKeys.Shift) == ModifierKeys.Shift; }
        }
    }

    public class SortConstructedEventArgs : EventArgs
    {
        public List<SortDescription> SortDescriptions { get; private set; }

        public SortConstructedEventArgs(List<SortDescription> sortStack)
        {
            SortDescriptions = sortStack;
        }

        public IOrderedQueryable<T> Order<T>(IQueryable<T> queryable)
        {
            if (SortDescriptions == null || SortDescriptions.Count == 0)
                return queryable.OrderBy(x => 0);

            IOrderedQueryable<T> result;


            var first = SortDescriptions.First();
            switch (first.Direction)
            {
                case ListSortDirection.Ascending:
                    result = queryable.OrderBy(first.PropertyName); // uses orderby extension methods!
                    break;

                case ListSortDirection.Descending:
                    result = queryable.OrderByDescending(first.PropertyName);
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }

            foreach (var sort in SortDescriptions.Skip(1))
            {
                switch (sort.Direction)
                {
                    case ListSortDirection.Ascending:
                        result = result.ThenBy(sort.PropertyName);
                        break;
                    case ListSortDirection.Descending:
                        result = result.ThenByDescending(sort.PropertyName);
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            return result;
        }
    }
}