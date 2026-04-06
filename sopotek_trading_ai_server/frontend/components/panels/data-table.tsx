import { ReactNode } from "react";

type Column<T> = {
  key: string;
  header: string;
  render: (row: T) => ReactNode;
};

type DataTableProps<T> = {
  rows: T[];
  columns: Column<T>[];
};

export function DataTable<T>({ rows, columns }: DataTableProps<T>) {
  return (
    <div className="overflow-hidden rounded-[22px] border border-white/10">
      <table className="min-w-full divide-y divide-white/10 text-left">
        <thead className="bg-white/5 text-xs uppercase tracking-[0.24em] text-mist/45">
          <tr>
            {columns.map((column) => (
              <th key={column.key} className="px-4 py-3 font-medium">
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-white/10 text-sm text-mist">
          {rows.map((row, index) => (
            <tr key={index} className="bg-black/10">
              {columns.map((column) => (
                <td key={column.key} className="px-4 py-3 align-top">
                  {column.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
